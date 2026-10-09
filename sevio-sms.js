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
  // The CLI's own -t is expected to fire first.
  const KILL_TIMEOUT_MS = 35000;

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
  const callUbus = (bin, socketPath, timeoutSec, payload, callback) => {
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
          finish(failure('ubus_timeout', `ubus: killed after ${KILL_TIMEOUT_MS}ms`));
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

    killTimer = setTimeout(() => child.kill(), KILL_TIMEOUT_MS);

    return child;
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
    let closed = false;

    node.status({ fill: 'blue', shape: 'dot', text: 'ready' });

    node.on('input', function (msg, send, done) {
      send = send || node.send.bind(node);
      done = done || function () {};

      const fail = (code, message) => {
        if (closed) return;
        node.status({ fill: 'red', shape: 'dot', text: code });
        node.error(message, msg);
        send([null, Object.assign({}, msg, { payload: { ok: false, code: code, message: message } })]);
        done();
      };

      const payload = msg.payload;
      const alertId = payload && payload.alertId;
      const body = payload && payload.body;

      if (typeof alertId !== 'string' || alertId === '' || typeof body !== 'string') {
        fail('invalid_payload', 'msg.payload must be { alertId, body }');
        return;
      }

      node.status({ fill: 'blue', shape: 'ring', text: 'sending' });

      readRecipients(recipientsFile, alertId, (err, to) => {
        if (closed) return;
        if (err) {
          fail(err.code, err.message);
          return;
        }

        // One ubus call per recipient: rpcd's sms service has a 30 s exec
        // timeout and a single send can take ~22 s worst case, so a combined
        // call could be killed mid-flight. Fail-fast on the first recipient
        // that fails; only report success once all of them have succeeded.
        const refs = [];
        const sendNext = (index) => {
          if (closed) return;
          if (index >= to.length) {
            const out = Object.assign({}, msg, { payload: { ok: true } });
            if (refs.length > 0) {
              out.payload.ref = refs.join(',');
            }
            node.status({ fill: 'green', shape: 'dot', text: 'sent' });
            send([out, null]);
            done();
            return;
          }

          const child = callUbus(ubusBin, ubusSocket, UBUS_TIMEOUT_SEC, { to: [to[index]], body: body }, (ubusErr, result) => {
            children.delete(child);
            if (closed) return;
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
              sendNext(index + 1);
            } else {
              const code = reply.code || 'send_failed';
              const message = reply.message || 'sms send failed';
              logSend(loggerBin, loggerTag, alertId, index, to.length, code, message);
              fail(code, message);
            }
          });
          children.add(child);
        };

        sendNext(0);
      });
    });

    node.on('close', function (removed, done) {
      closed = true;
      for (const child of children) {
        child.kill();
      }
      children.clear();
      done();
    });
  }

  RED.nodes.registerType('sevio-sms', SevioSmsNode);
};
