module.exports = (RED) => {
  const fs = require('fs');
  const { execFile } = require('child_process');

  const RECIPIENTS_FILE = '/tmp/datagateway-alert-recipients.json';
  // The ubus CLI talks to ubusd directly (its default socket), defaults to a
  // 30 s call timeout and does not block other ubus clients. The previous
  // JSON-RPC proxy socket (`ubus-json-server`) hardcoded a 5000 ms timeout,
  // shorter than an SMS send, so slow sends failed with ubus status 7
  // (UBUS_STATUS_TIMEOUT) as `{"ok":false,"code":7}`.
  const UBUS_TIMEOUT_SEC = 30;
  // Node-side backstop: kill the child if the CLI has not returned by then.
  // The CLI's own -t is expected to fire first. SIGKILL, not the SIGTERM
  // default: a wedged ubus must not be able to ignore the signal and keep
  // holding the process-wide queue slot. Overridable via `config.killTimeoutMs`
  // or `SEVIO_SMS_KILL_TIMEOUT_MS`.
  const KILL_TIMEOUT_MS = 35000;
  // Watchdog on a held queue slot: force-release it if a run never reaches a
  // terminal path. A `busy` answer is not instant on the rpcd side: rpcd waits
  // up to LOCK_TRIES=15 x LOCK_SLEEP=0.2 s = 3 s for the modem lock before it
  // replies, and the node then waits BUSY_RETRY_DELAY_MS before the next
  // attempt, so the per-recipient worst case is
  // 2 x (3 s + 1 s) + 35 s = 43 s. Alert fan-outs are contact lists (1-3 in
  // the flow's own tests), so 240 s comfortably exceeds any plausible run
  // while staying below the alert flow's 300 s `alertBusy` stale guard
  // (ALERT_BUSY_TIMEOUT_MS in datagateway). That ordering only holds while
  // the flow's modbus latency plus the waiting alert's queue wait stay within
  // the 60 s margin; the 30 s default queue wait leaves 30 s of slack.
  // Overridable via `config.slotMaxMs` or `SEVIO_SMS_SLOT_MAX_MS`.
  const SLOT_MAX_MS = 240000;
  // How long an input may wait for the slot before failing with
  // `queue_timeout`. The deadline covers the wait only, never a started send.
  const DEFAULT_QUEUE_WAIT_MS = 30000;
  // setTimeout silently turns anything outside (0, MAX_TIMER_MS] into a ~1 ms
  // timer (large values are clamped, negative/NaN coerced), so a bad
  // queueWaitMs would make every contended alert fail `queue_timeout`
  // instantly instead of waiting.
  const MAX_TIMER_MS = 2147483647;
  const normalizeQueueWaitMs = (value) => {
    const ms = Number(value);
    return Number.isFinite(ms) && ms > 0 && ms <= MAX_TIMER_MS ? ms : DEFAULT_QUEUE_WAIT_MS;
  };
  // rpcd answers `busy` before rate_commit/gcom, so retrying it is
  // side-effect-free and costs no rate budget. Every other code is terminal:
  // retrying an ambiguous outcome could send the same SMS twice.
  const BUSY_MAX_RETRIES = 2;
  const BUSY_RETRY_DELAY_MS = 1000;

  const failure = (code, message) => {
    const err = new Error(message);
    err.code = code;
    return err;
  };

  const oneLine = (value) => String(value).replace(/\s*\n\s*/g, ' ');

  /**
   * Reports one send attempt and its outcome to syslog through the `logger`
   * CLI (busybox). Node-RED's stdout is buffered/bursty on the device, so
   * node.error/node.warn alone do not make a failed send diagnosable from
   * syslog. The recipient is named by its position in the fan-out (`to=1/2`),
   * never by number — an MSISDN is never logged. Fire-and-forget: a missing
   * or failing `logger` must never affect the send. `ref`/`message` are only
   * included when present.
   */
  const logSend = (bin, tag, alertId, index, total, outcome, message, ref) => {
    let line = `alert=${oneLine(alertId)} to=${index + 1}/${total} outcome=${outcome}`;
    if (ref !== undefined && ref !== null) {
      line += ` ref=${oneLine(ref)}`;
    }
    if (message !== undefined && message !== null && String(message).trim() !== '') {
      line += ` message=${oneLine(message)}`;
    }
    try {
      execFile(bin, ['-t', tag, line], () => {});
    } catch (err) {
      // Logging must never break a send.
    }
  };

  /**
   * Resolves recipient numbers for an alert from the datagateway-written file.
   * The resolved numbers are only ever passed to the ubus call — never logged,
   * never included in status or error messages.
   */
  const readRecipients = (file, alertId, callback) => {
    fs.readFile(file, 'utf8', (err, data) => {
      if (err) {
        callback(failure('recipients_unavailable', `cannot read ${file}: ${err.code || err.message}`));
        return;
      }
      let map;
      try {
        map = JSON.parse(data);
      } catch (parseErr) {
        callback(failure('recipients_unavailable', `invalid JSON in ${file}`));
        return;
      }
      if (!map || typeof map !== 'object' || Array.isArray(map)) {
        callback(failure('recipients_unavailable', `invalid recipients file content: ${file}`));
        return;
      }
      const to = map[alertId];
      if (!Array.isArray(to) || to.length === 0) {
        callback(failure('no_recipients', `no recipients for alert ${alertId}`));
        return;
      }
      callback(null, to);
    });
  };

  /**
   * Calls the `ubus` CLI:
   *   ubus [-s <socket>] [-t <timeout>] call sms send '<payload JSON>'
   * Exit 0: the method result is printed as JSON on stdout (an rpcd-level
   * error is still exit 0 with a `{"ok":false,"code":...}` body).
   * Exit >= 128: ubus-level error, exit code = 256 - <ubus status>, stdout
   * empty and a diagnostic on stderr (249 = timeout, 253 = method not found).
   * Spawn failure (ENOENT): the binary is not available.
   * Returns the child process so the node can kill in-flight calls on close.
   */
  const callUbus = (bin, socketPath, timeoutSec, killTimeoutMs, payload, callback) => {
    const args = ['-t', String(timeoutSec)];
    if (socketPath) {
      args.push('-s', socketPath);
    }
    args.push('call', 'sms', 'send', JSON.stringify(payload));

    let done = false;
    let killTimer = null;

    const finish = (err, value) => {
      if (done) return;
      done = true;
      if (killTimer) clearTimeout(killTimer);
      callback(err, value);
    };

    const child = execFile(bin, args, { encoding: 'utf8' }, (err, stdout, stderr) => {
      const stderrText = (stderr || '').trim();
      if (err) {
        if (err.code === 'ENOENT') {
          finish(failure('ubus_unavailable', `ubus: cannot execute ${bin}: ${err.message}`));
        } else if (typeof err.code === 'number' && err.code >= 128) {
          const status = 256 - err.code;
          finish(failure(
            status === 7 ? 'ubus_timeout' : 'ubus_error',
            stderrText || `ubus call failed with status ${status}`
          ));
        } else if (err.signal || err.killed) {
          finish(failure('ubus_timeout', `ubus: killed after ${killTimeoutMs}ms`));
        } else {
          finish(failure('ubus_error', stderrText || `ubus: ${err.message}`));
        }
        return;
      }

      let reply;
      try {
        reply = JSON.parse(stdout);
      } catch (parseErr) {
        finish(failure('ubus_error', 'ubus: invalid JSON response'));
        return;
      }
      if (!reply || typeof reply !== 'object' || Array.isArray(reply)) {
        finish(failure('ubus_error', 'ubus: invalid JSON response'));
        return;
      }
      finish(null, reply);
    });

    killTimer = setTimeout(() => child.kill('SIGKILL'), killTimeoutMs);

    return child;
  };

  // ---------------------------------------------------------------------------
  // Process-wide send queue
  //
  // This factory body runs once per Node-RED process (the module is required
  // once and the factory called once per node set), so `waiters`/`held` below
  // serialise every `sevio-sms` node in the process: at most one alert
  // fan-out touches the modem at a time. A slot holds a whole fan-out, not a
  // single ubus call — the fan-out is one logical alert and its per-recipient
  // calls must not interleave with another alert's.
  // ---------------------------------------------------------------------------
  const waiters = [];
  let held = false;

  const pump = () => {
    if (held || waiters.length === 0) return;
    const waiter = waiters.shift();
    clearTimeout(waiter.timer); // the wait is over: queueWaitMs bounds waiting only, never the send
    held = true;
    let released = false;
    const watchdog = setTimeout(release, waiter.slotMaxMs);
    // Idempotent: success, every failure, every closed early-return and the
    // watchdog can all call it; only the first call frees the slot.
    function release() {
      if (released) return;
      released = true;
      clearTimeout(watchdog);
      held = false;
      pump();
    }
    // `isReleased()` turns true when the watchdog reclaimed the slot under a
    // still-running input: the run must notice and stop sending to later
    // recipients instead of overlapping the alert that holds the slot now.
    waiter.start(release, () => released);
  };

  /**
   * Queues `start(release, isReleased)` for the next free slot. `owner` is the
   * node instance: its close sweeps out not-yet-started waiters by identity, so
   * a closed node can never start a send. `slotMaxMs` is the watchdog budget
   * for the acquired slot (each node passes its own; the shared pump has no
   * node config). `onTimeout` fires if the turn does not come within `waitMs`,
   * with the waiter already removed from the queue and no slot ever held.
   */
  const acquire = (owner, waitMs, start, onTimeout, slotMaxMs) => {
    const waiter = { owner: owner, start: start, slotMaxMs: slotMaxMs || SLOT_MAX_MS };
    waiter.timer = setTimeout(() => {
      const i = waiters.indexOf(waiter);
      // Already started or swept by close (both clear the timer first): the
      // turn is gone, so failing the input here could done() an input that is
      // not waiting any more.
      if (i < 0) return;
      waiters.splice(i, 1);
      onTimeout();
    }, waitMs);
    waiters.push(waiter);
    pump();
  };

  function SevioSmsNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    // Overridable in tests (`config.*`) and on the device (`SEVIO_SMS_*`);
    // production defaults are the bare `ubus` CLI on PATH and no `-s`, since
    // the CLI's default socket is the real ubusd one.
    const recipientsFile = config.recipientsFile || RECIPIENTS_FILE;
    const ubusBin = config.ubusBin || process.env.SEVIO_SMS_UBUS_BIN || 'ubus';
    const envSocket = process.env.SEVIO_SMS_UBUS_SOCKET;
    const ubusSocket = config.ubusSocket || envSocket || '';
    // Send outcomes (and only outcomes) are reported to syslog through the
    // `logger` CLI; binary and tag are overridable like the ubus settings.
    const loggerBin = config.loggerBin || process.env.SEVIO_SMS_LOGGER_BIN || 'logger';
    const loggerTag = config.loggerTag || process.env.SEVIO_SMS_LOGGER_TAG || 'sevio-sms';
    // How long an input may wait for the process-wide slot before failing
    // `queue_timeout`: node config beats env beats the 30 s default. Only a
    // finite number in (0, 2147483647] is accepted; anything else falls back
    // to the default rather than becoming a ~1 ms timer.
    const queueWaitMs = normalizeQueueWaitMs(
      config.queueWaitMs !== undefined ? config.queueWaitMs : process.env.SEVIO_SMS_QUEUE_WAIT_MS
    );
    // Watchdog budget for a held slot (see SLOT_MAX_MS) and Node-side backstop
    // for a hung ubus child (see KILL_TIMEOUT_MS). Config beats env beats the
    // default; slotMaxMs travels with each queue wait because the shared pump
    // has no node config.
    const slotMaxMs = config.slotMaxMs || Number(process.env.SEVIO_SMS_SLOT_MAX_MS) || SLOT_MAX_MS;
    const killTimeoutMs = config.killTimeoutMs || Number(process.env.SEVIO_SMS_KILL_TIMEOUT_MS) || KILL_TIMEOUT_MS;
    // An env override silently redirects every alert away from the default
    // socket; warn once (per node, at construction) so a misconfigured device
    // is diagnosable from the flow editor. An explicit node config is visible
    // in the flow itself, so only the env-var path warns. Path only — recipient
    // data is never logged.
    if (!config.ubusSocket && envSocket) {
      const warning = `sevio-sms: using ubus socket ${envSocket} from SEVIO_SMS_UBUS_SOCKET`;
      if (typeof node.warn === 'function') {
        node.warn(warning);
      } else {
        console.warn(warning);
      }
    }
    const children = new Set();
    // Inputs waiting out the BUSY_RETRY_DELAY_MS retry timer, with their slot's
    // drop(): on close the timer is cancelled and the slot released at once,
    // instead of the callback holding the process-wide slot for up to a second
    // after the node is gone.
    const retryTimers = new Set();
    let closed = false;

    node.status({ fill: 'blue', shape: 'dot', text: 'ready' });

    node.on('input', function (msg, send, done) {
      send = send || node.send.bind(node);
      done = done || function () {};

      // `release` is the queue slot acquired for this input. release() is
      // idempotent and must be called on every terminal path — success, every
      // failure and the closed early-returns — so a failed or aborted send can
      // never wedge the process-wide queue. `slotReleased()` is true once the
      // slot watchdog has freed the slot under this input.
      let release = null;
      let slotReleased = null;

      const fail = (code, message) => {
        if (release) release();
        if (closed) return;
        node.status({ fill: 'red', shape: 'dot', text: code });
        node.error(message, msg);
        send([null, Object.assign({}, msg, { payload: { ok: false, code: code, message: message } })]);
        done();
      };

      const succeed = (refs) => {
        if (release) release();
        if (closed) return;
        const out = Object.assign({}, msg, { payload: { ok: true } });
        if (refs.length > 0) {
          out.payload.ref = refs.join(',');
        }
        node.status({ fill: 'green', shape: 'dot', text: 'sent' });
        send([out, null]);
        done();
      };

      // A slot held when the node closes is dropped without any output or
      // done(): Node-RED has already closed the node and drops both.
      const drop = () => {
        if (release) release();
      };

      const payload = msg.payload;
      const alertId = payload && payload.alertId;
      const body = payload && payload.body;

      if (typeof alertId !== 'string' || alertId === '' || typeof body !== 'string') {
        fail('invalid_payload', 'msg.payload must be { alertId, body }');
        return;
      }

      node.status({ fill: 'blue', shape: 'ring', text: 'sending' });

      acquire(node, queueWaitMs, function (slotRelease, isReleased) {
        release = slotRelease;
        slotReleased = isReleased;
        if (closed) {
          drop();
          return;
        }

        // Recipients are resolved inside the slot: a queued alert reads the
        // file at send time, not at submission time.
        readRecipients(recipientsFile, alertId, (err, to) => {
          if (closed) {
            drop();
            return;
          }
          if (slotReleased && slotReleased()) {
            return;
          }
          if (err) {
            fail(err.code, err.message);
            return;
          }

          // One ubus call per recipient: rpcd's sms service has a 30 s exec
          // timeout and a single send can take ~22 s worst case, so a combined
          // call could be killed mid-flight. Fail-fast on the first recipient
          // that fails; only report success once all of them have succeeded.
          const refs = [];
          const sendNext = (index, busyRetries) => {
            if (closed) {
              drop();
              return;
            }
            // The watchdog already reclaimed the slot for the next alert: an
            // abandoned run must not send to another recipient (it would
            // overlap the alert that holds the slot now).
            if (slotReleased && slotReleased()) {
              return;
            }
            if (index >= to.length) {
              succeed(refs);
              return;
            }

            const child = callUbus(ubusBin, ubusSocket, UBUS_TIMEOUT_SEC, killTimeoutMs, { to: [to[index]], body: body }, (ubusErr, result) => {
              children.delete(child);
              if (closed) {
                drop();
                return;
              }
              if (slotReleased && slotReleased()) {
                return;
              }
              if (ubusErr) {
                logSend(loggerBin, loggerTag, alertId, index, to.length, ubusErr.code, ubusErr.message);
                fail(ubusErr.code, ubusErr.message);
                return;
              }

              const reply = result && typeof result === 'object' ? result : {};
              if (reply.ok) {
                let ref = null;
                if (reply.ref !== undefined && reply.ref !== null) {
                  ref = reply.ref;
                  refs.push(reply.ref);
                }
                logSend(loggerBin, loggerTag, alertId, index, to.length, 'ok', null, ref);
                sendNext(index + 1, 0);
                return;
              }

              const code = reply.code || 'send_failed';
              const message = reply.message || 'sms send failed';
              logSend(loggerBin, loggerTag, alertId, index, to.length, code, message);
              // `busy` is answered before rate_commit/gcom, so it is
              // side-effect-free and costs no rate budget: retry it a bounded
              // number of times, per recipient. Every other failure is
              // terminal — retrying an ambiguous outcome risks a duplicate SMS.
              if (code === 'busy' && busyRetries < BUSY_MAX_RETRIES) {
                const entry = { timer: null, drop: drop };
                entry.timer = setTimeout(() => {
                  retryTimers.delete(entry);
                  sendNext(index, busyRetries + 1);
                }, BUSY_RETRY_DELAY_MS);
                retryTimers.add(entry);
                return;
              }
              fail(code, message);
            });
            children.add(child);
          };

          sendNext(0, 0);
        });
      }, () => {
        fail('queue_timeout', `sms queue wait exceeded ${queueWaitMs}ms`);
      }, slotMaxMs);
    });

    node.on('close', function (removed, done) {
      closed = true;
      // Cancel this node's queued-but-not-started waiters: a closed node must
      // never start a send. In-flight children are killed below; their
      // callbacks land on the closed early-returns above and free the slot.
      for (let i = waiters.length - 1; i >= 0; i -= 1) {
        if (waiters[i].owner === node) {
          clearTimeout(waiters[i].timer);
          waiters.splice(i, 1);
        }
      }
      // A close during the busy-retry delay must free the slot now, not when
      // the (up to 1 s) retry timer would have fired.
      for (const retry of retryTimers) {
        clearTimeout(retry.timer);
        retry.drop();
      }
      retryTimers.clear();
      for (const child of children) {
        child.kill();
      }
      children.clear();
      done();
    });
  }

  RED.nodes.registerType('sevio-sms', SevioSmsNode);
};
