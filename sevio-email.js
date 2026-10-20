/**
 * `sevio-email` — sends one alert to every email recipient of `alertId`
 * through `msmtp`, one invocation per recipient, sequentially, fail-fast.
 *
 * Contract (alert-email spec §3.3-§3.6):
 * - input `{ alertId, subject, body }`; output 1 `{ ok: true, accepted: n }`
 *   **only** when every recipient's message was accepted by the relay; output 2
 *   `{ ok: false, code, message, accepted: n, total: m }` otherwise;
 * - the recipients artefact is read **at send time inside the slot**, and an
 *   entry must carry `channel: "email"`;
 * - one `msmtp` invocation per recipient, shell-free and option-terminated
 *   (`--file=<rendered config>` then `--` then the address), never a CC list;
 * - the message is composed here and written to the child's stdin, never
 *   interpolated into argv;
 * - retry only what is definitively **not accepted** (a connection failure or
 *   an explicit 4xx deferral). A lost final `250` after the message data is
 *   `outcome_unknown` and is never retried: a retry can duplicate mail;
 * - the binary comes only from node configuration or the environment, never
 *   from a message.
 *
 * The transport lives in exactly one function (`invokeMsmtp`): the production
 * send and every retry re-execution go through it, so the argv, the address
 * validation, the credential-file reference and the deadlines cannot drift.
 */

// The shared constants of §3.6. They are exported (below) so the builder in
// datagateway can be checked against the same numbers, and so the arithmetic
// itself is testable. This package has no build step linking it to
// datagateway's copy; the test asserts the derivation, and a drift on either
// side is a test failure on that side.
const PER_ATTEMPT_DEADLINE_MS = 30000; // end-to-end, including connect
const ATTEMPTS = 2;
const BACKOFF_MS = 2000;
const FANOUT_CAP = 5;
const QUEUE_WAIT_MS = 30000;
const TERMINATION_GRACE_MS = 5000;
const MARGIN_MS = 30000;
// per recipient = ATTEMPTS x PER_ATTEMPT_DEADLINE + (ATTEMPTS - 1) x BACKOFF
//              = 2 x 30 s + 2 s = 62 s
const PER_RECIPIENT_MS = ATTEMPTS * PER_ATTEMPT_DEADLINE_MS + (ATTEMPTS - 1) * BACKOFF_MS;
// slot bound = FANOUT_CAP x 62 s + TERMINATION_GRACE = 310 s + 5 s = 315 s.
// TERMINATION_GRACE is counted once, here and nowhere else.
const SLOT_MAX_MS = FANOUT_CAP * PER_RECIPIENT_MS + TERMINATION_GRACE_MS;
// sendingDeadlineMs = QUEUE_WAIT + slot bound + MARGIN = 30 + 315 + 30 = 375 s
const SENDING_DEADLINE_MS = QUEUE_WAIT_MS + SLOT_MAX_MS + MARGIN_MS;

const CONSTANTS = {
  PER_ATTEMPT_DEADLINE_MS: PER_ATTEMPT_DEADLINE_MS,
  ATTEMPTS: ATTEMPTS,
  BACKOFF_MS: BACKOFF_MS,
  FANOUT_CAP: FANOUT_CAP,
  QUEUE_WAIT_MS: QUEUE_WAIT_MS,
  TERMINATION_GRACE_MS: TERMINATION_GRACE_MS,
  MARGIN_MS: MARGIN_MS,
  PER_RECIPIENT_MS: PER_RECIPIENT_MS,
  SLOT_MAX_MS: SLOT_MAX_MS,
  SENDING_DEADLINE_MS: SENDING_DEADLINE_MS
};

module.exports = (RED) => {
  const fs = require('fs');
  const { execFile } = require('child_process');

  const RECIPIENTS_FILE = '/tmp/datagateway-alert-recipients.json';
  const EMAIL_CHANNEL = 'email';
  // setTimeout silently turns anything outside (0, MAX_TIMER_MS] into a ~1 ms
  // timer, so a bad queueWaitMs would make every contended alert fail
  // `queue_timeout` instantly instead of waiting.
  const MAX_TIMER_MS = 2147483647;

  const normalizeTimerMs = (value, fallback) => {
    const ms = Number(value);
    return Number.isFinite(ms) && ms > 0 && ms <= MAX_TIMER_MS ? ms : fallback;
  };

  const failure = (code, message) => {
    const err = new Error(message);
    err.code = code;
    return err;
  };

  const oneLine = (value) => String(value).replace(/\s*\n\s*/g, ' ');

  /**
   * Reports one send attempt and its outcome to syslog through the `logger`
   * CLI. The recipient is named by its position in the fan-out (`to=1/2`),
   * never by address: an email address is never logged, and msmtp's stderr
   * (which quotes addresses and protocol details) is never logged either.
   * Fire-and-forget: a missing or failing `logger` must never affect a send.
   */
  const logSend = (bin, tag, alertId, index, total, attempt, outcome) => {
    const line = `alert=${oneLine(alertId)} to=${index + 1}/${total} attempt=${attempt}/${ATTEMPTS} outcome=${outcome}`;
    try {
      execFile(bin, ['-t', tag, line], () => {});
    } catch (err) {
      // Logging must never break a send.
    }
  };

  // Names the channel of a rejected entry for the error message without ever
  // rendering a non-string as `[object Object]` or `undefined`.
  const channelName = (channel) => (
    typeof channel === 'string' && channel !== '' ? JSON.stringify(channel) : 'missing'
  );

  /**
   * Resolves recipient addresses for an alert from the datagateway-written
   * file. Addresses are only ever passed to the transport — never logged,
   * never included in status or error messages.
   *
   * The artefact is channel-keyed: `{ <alertId>: { channel, targets } }`.
   * Email **rejects** the legacy bare array (it means `channel: "sms"`) and
   * requires `channel: "email"`: a missing, unknown or different channel, a
   * non-object entry or the legacy array all fail closed with
   * `channel_mismatch` — never a silent skip and never a fallback to the
   * other transport. A missing or unusable `targets` list on the email
   * channel is `no_recipients`: a targets problem, not a channel problem, so
   * the code must not lie in the logs (spec §3.3).
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
      const entry = map[alertId];
      if (entry === undefined || entry === null) {
        callback(failure('no_recipients', `no recipients for alert ${alertId}`));
        return;
      }
      // The legacy bare array is the sms channel: email refuses it.
      if (Array.isArray(entry)) {
        callback(failure(
          'channel_mismatch',
          `alert ${alertId} recipients are for channel "sms" (legacy array), not ${EMAIL_CHANNEL}`
        ));
        return;
      }
      if (typeof entry !== 'object') {
        callback(failure(
          'channel_mismatch',
          `invalid recipient entry for alert ${alertId}: expected a { channel, targets } object for ${EMAIL_CHANNEL}`
        ));
        return;
      }
      if (entry.channel !== EMAIL_CHANNEL) {
        callback(failure(
          'channel_mismatch',
          `alert ${alertId} recipients are for channel ${channelName(entry.channel)}, not ${EMAIL_CHANNEL}`
        ));
        return;
      }
      if (!Array.isArray(entry.targets) || entry.targets.length === 0) {
        callback(failure('no_recipients', `alert ${alertId} has no usable targets list`));
        return;
      }
      callback(null, entry.targets);
    });
  };

  // ---------------------------------------------------------------------------
  // Address validation — the node's last check before it builds an `msmtp`
  // argument. The contract is deliberately narrower than RFC 5322: one
  // addr-spec, no display name, no angle brackets, no quoted local part, no
  // groups. The rejected characters are the ones that could escape a header
  // or a recipient list — CR, LF, NUL, whitespace, `,`, `;`, `<`, `>` — plus
  // a leading `-`, which msmtp would otherwise parse as an option. `--`
  // terminates the options as well; both defences are required (spec §3.4).
  // Messages name the rule, never echo the value: an address can itself be an
  // injection attempt.
  // ---------------------------------------------------------------------------
  const EMAIL_ADDRESS_MAX_LENGTH = 254;
  const LOCAL_PART_MAX_LENGTH = 64;
  const DOMAIN_MAX_LENGTH = 253;
  const LABEL_MAX_LENGTH = 63;
  const FORBIDDEN_PUNCTUATION = /[\s,;<>]/;
  const LOCAL_PART = /^[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*$/;
  const DOMAIN_LABEL = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/;
  // eslint-disable-next-line no-control-regex
  const FORBIDDEN_CONTROL = /[\u0000\r\n]/;

  const addressProblem = (value) => {
    if (typeof value !== 'string' || value.length === 0) return 'is not a non-empty string';
    if (value.length > EMAIL_ADDRESS_MAX_LENGTH) {
      return `is longer than ${EMAIL_ADDRESS_MAX_LENGTH} characters`;
    }
    if (FORBIDDEN_CONTROL.test(value) || FORBIDDEN_PUNCTUATION.test(value)) {
      return 'must be a single address with no display name, whitespace, comma, semicolon or angle brackets';
    }
    if (value.startsWith('-')) return 'must not begin with "-"';

    const at = value.indexOf('@');
    if (at === -1 || at !== value.lastIndexOf('@')) {
      return 'must be a single address in the form local@domain';
    }
    const local = value.slice(0, at);
    const domain = value.slice(at + 1);

    if (local.length === 0) return 'must have a local part before "@"';
    if (local.length > LOCAL_PART_MAX_LENGTH) {
      return `must have a local part of at most ${LOCAL_PART_MAX_LENGTH} characters`;
    }
    if (!LOCAL_PART.test(local)) return 'has an invalid local part';

    if (domain.length === 0) return 'must have a domain after "@"';
    if (domain.length > DOMAIN_MAX_LENGTH) {
      return `must have a domain of at most ${DOMAIN_MAX_LENGTH} characters`;
    }
    if (!domain.includes('.')) return 'must have a fully-qualified domain';
    for (const label of domain.split('.')) {
      if (label.length === 0) return 'must not contain an empty domain label';
      if (label.length > LABEL_MAX_LENGTH) {
        return `must have domain labels of at most ${LABEL_MAX_LENGTH} characters`;
      }
      if (!DOMAIN_LABEL.test(label)) return 'has an invalid domain label';
    }
    return null;
  };

  const pad2 = (n) => String(n).padStart(2, '0');

  /**
   * RFC 5322 date-time in the local zone, e.g. `Tue, 20 Oct 2026 16:38:21 +0200`.
   * Written by hand so the node needs no locale: `toLocaleString` is not
   * guaranteed on the device's minimal Node build and its output is not a
   * header.
   */
  const formatDate = (date) => {
    const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const offset = -date.getTimezoneOffset();
    const sign = offset >= 0 ? '+' : '-';
    const abs = Math.abs(offset);
    return (
      `${days[date.getDay()]}, ${pad2(date.getDate())} ${months[date.getMonth()]} ${date.getFullYear()} ` +
      `${pad2(date.getHours())}:${pad2(date.getMinutes())}:${pad2(date.getSeconds())} ` +
      `${sign}${pad2(Math.floor(abs / 60))}${pad2(abs % 60)}`
    );
  };

  /**
   * Encodes a Subject header value. An ASCII subject is written as-is (CR, LF
   * and NUL are refused before this point); anything else is RFC 2047
   * Base64-encoded, one or more encoded words folded onto continuation lines.
   * Each word is at most 75 characters and never splits a UTF-8 sequence, so
   * the header is well-formed for any subject the API accepts.
   */
  const encodeSubject = (subject) => {
    if (/^[\x20-\x7e]*$/.test(subject)) return subject;
    const words = [];
    let chunk = '';
    for (const ch of subject) {
      if (Buffer.byteLength(chunk + ch, 'utf8') > 45) {
        words.push(chunk);
        chunk = '';
      }
      chunk += ch;
    }
    if (chunk !== '') words.push(chunk);
    return words
      .map((word) => `=?UTF-8?B?${Buffer.from(word, 'utf8').toString('base64')}?=`)
      .join('\r\n ');
  };

  /**
   * Composes the plain-text MIME message. Every header value is either
   * validated (from, to), encoded (subject) or generated here (date): no
   * value from the message is ever interpolated raw into a header. The body
   * is Base64 so any UTF-8 text survives a line-oriented transport intact,
   * and it is wrapped at 76 characters.
   */
  const composeMessage = (from, to, subject, body) => {
    const base64Body = Buffer.from(body, 'utf8').toString('base64');
    const wrapped = base64Body.match(/.{1,76}/g).join('\r\n');
    return [
      `From: ${from}`,
      `To: ${to}`,
      `Subject: ${encodeSubject(subject)}`,
      `Date: ${formatDate(new Date())}`,
      'MIME-Version: 1.0',
      'Content-Type: text/plain; charset=UTF-8',
      'Content-Transfer-Encoding: base64',
      '',
      wrapped
    ].join('\r\n') + '\r\n';
  };

  // msmtp prints the server's reply as `msmtp: server message: <code> <text>`
  // and its own connection failures as `msmtp: cannot connect to ...`. The
  // stderr is classified here and then discarded: it is never logged, because
  // it quotes the recipient address and the protocol dialogue.
  const SERVER_REPLY = /server message:\s*([245]\d\d)\b/g;
  const CONNECT_FAILURE = /cannot connect to|Network is unreachable|No route to host|Temporary failure in name resolution|Name or service not known/i;
  const TLS_FAILURE = /TLS|SSL|certificate/i;
  const AUTH_FAILURE = /authentication failed|authorization failed|invalid.*credentials/i;

  const lastServerReply = (stderr) => {
    let code = null;
    let match;
    SERVER_REPLY.lastIndex = 0;
    while ((match = SERVER_REPLY.exec(stderr)) !== null) {
      code = Number(match[1]);
    }
    return code;
  };

  /**
   * Decides what one exited child means, and — separately — whether a fresh
   * attempt is allowed. Retry safety is never inferred from a generic
   * temporary exit: msmtp's EX_TEMPFAIL (75) after the message data is exactly
   * the lost-`250` case, which must be `outcome_unknown`, not a retry.
   *
   * - an explicit 5xx is a permanent rejection; an explicit 4xx is a deferral
   *   and is retried (SMTP says a 4xx is not an acceptance);
   * - an authentication or configuration failure is permanent;
   * - a connection failure (`cannot connect to …`) or a TLS handshake failure
   *   happens before the message can be accepted, so it is retryable;
   * - a child killed at the deadline, or one that exited before the whole
   *   message was written to its stdin, cannot have sent the terminating dot:
   *   the message was truncated, so it was definitively not accepted and the
   *   attempt is retryable;
   * - anything else after the whole message was handed over is ambiguous:
   *   `outcome_unknown`, never retried, because a retry can duplicate mail.
   */
  const classifyAttempt = (exit) => {
    if (exit.spawnCode === 'ENOENT') {
      return { code: 'email_unavailable', retry: false };
    }
    const stderr = exit.stderr || '';
    const reply = lastServerReply(stderr);
    if (reply !== null && reply >= 500) return { code: 'rejected', retry: false };
    if (reply !== null && reply >= 400) return { code: 'deferred', retry: true };
    if (AUTH_FAILURE.test(stderr)) return { code: 'rejected', retry: false };
    if (exit.exitCode === 77 || exit.exitCode === 78 || exit.exitCode === 64 || exit.exitCode === 66) {
      return { code: 'rejected', retry: false };
    }
    if (CONNECT_FAILURE.test(stderr)) return { code: 'deferred', retry: true };
    if (TLS_FAILURE.test(stderr)) return { code: 'deferred', retry: true };
    if (exit.deadlineHit) {
      return exit.stdinFinished
        ? { code: 'outcome_unknown', retry: false }
        : { code: 'deferred', retry: true };
    }
    if (!exit.stdinFinished) return { code: 'deferred', retry: true };
    return { code: 'outcome_unknown', retry: false };
  };

  const describeFailure = (code, index, total, attemptsUsed) => {
    const head = `recipient ${index + 1}/${total}`;
    if (code === 'outcome_unknown') {
      return `${head} outcome unknown: the relay did not confirm acceptance, and a retry could duplicate the message`;
    }
    if (code === 'deferred') {
      return `${head} was not accepted by the relay: deferred after ${attemptsUsed} attempt(s)`;
    }
    if (code === 'email_unavailable') {
      return `${head} was not sent: msmtp is not available`;
    }
    return `${head} was rejected by the relay`;
  };

  /**
   * The one transport implementation. Every caller — the production send and
   * every retry — goes through here, so the argv, the option terminator, the
   * address validation and the deadline are identical on every path.
   *
   * `execFile(bin, ['--file=<rendered config>', '--', address])`: no shell, so
   * no splitting; `--` ends option parsing, so even an address that begins
   * with `-` (refused below anyway) could never become an option. The message
   * goes on stdin: neither the subject nor an address ever becomes argv, and
   * the password stays in the 0600 config file the argument points at.
   *
   * The deadline is per attempt and covers the whole child lifetime —
   * connection establishment included. Returns the child so `close` can kill
   * it; `callback(result)` fires exactly once.
   */
  const invokeMsmtp = (bin, configFile, from, address, subject, body, deadlineMs, callback) => {
    const problem = addressProblem(address);
    if (problem !== null) {
      callback({ invalidAddress: problem });
      return null;
    }

    const args = [`--file=${configFile}`, '--', address];
    const message = composeMessage(from, address, subject, body);
    let done = false;
    let stdinFinished = false;
    let deadlineHit = false;
    let deadlineTimer = null;

    const finish = (result) => {
      if (done) return;
      done = true;
      if (deadlineTimer) clearTimeout(deadlineTimer);
      callback(result);
    };

    let child;
    try {
      child = execFile(bin, args, { encoding: 'utf8' }, (err, stdout, stderr) => {
        finish({
          exitCode: err && typeof err.code === 'number' ? err.code : (err ? null : 0),
          signal: err ? err.signal : null,
          spawnCode: err && typeof err.code === 'string' ? err.code : null,
          stderr: stderr || '',
          stdinFinished: stdinFinished,
          deadlineHit: deadlineHit
        });
      });
    } catch (err) {
      finish({ spawnCode: err && err.code ? err.code : 'spawn_failed', stderr: '' });
      return null;
    }

    // The child may exit (or be killed) before consuming stdin: EPIPE must not
    // become an uncaught exception.
    child.stdin.on('error', () => {});
    // `finish` means every byte of the message reached the OS pipe: enough to
    // know the terminating dot was written, not that the relay answered.
    child.stdin.on('finish', () => { stdinFinished = true; });
    child.stdin.end(message);

    deadlineTimer = setTimeout(() => {
      deadlineHit = true;
      // SIGKILL, not SIGTERM: a wedged msmtp must not be able to ignore the
      // signal and keep holding the slot (same reasoning as the SMS node).
      child.kill('SIGKILL');
    }, deadlineMs);

    return child;
  };

  // ---------------------------------------------------------------------------
  // Send slot queue — PER NODE TYPE, not cross-channel
  //
  // This factory body runs once per Node-RED process (the module is required
  // once and the factory called once per node set), so `waiters`/`held` below
  // serialise every `sevio-email` node in the process: at most one email
  // fan-out touches the relay at a time. It is deliberately NOT shared with
  // `sevio-sms`: the two nodes have independent queues, and nothing here
  // claims a cross-channel bound. Inside a flow, overlap cannot arise in the
  // first place because the run is single-flight; the queue exists for
  // process-wide safety, not to enable concurrency.
  // ---------------------------------------------------------------------------
  const waiters = [];
  let held = false;

  const pump = () => {
    if (held || waiters.length === 0) return;
    const waiter = waiters.shift();
    clearTimeout(waiter.timer); // queue wait bounds waiting only, never the send
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
    waiter.start(release, () => released);
  };

  /**
   * Queues `start(release, isReleased)` for the next free slot. `owner` is the
   * node instance: its close sweeps out not-yet-started waiters by identity,
   * so a closed node can never start a send.
   */
  const acquire = (owner, waitMs, start, onTimeout, slotMaxMs) => {
    const waiter = { owner: owner, start: start, slotMaxMs: slotMaxMs || SLOT_MAX_MS };
    waiter.timer = setTimeout(() => {
      const i = waiters.indexOf(waiter);
      // Already started or swept by close (both clear the timer first).
      if (i < 0) return;
      waiters.splice(i, 1);
      onTimeout();
    }, waitMs);
    waiters.push(waiter);
    pump();
  };

  function SevioEmailNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const recipientsFile = config.recipientsFile || RECIPIENTS_FILE;
    // The binary comes only from node configuration or the environment. A
    // message can never choose what is executed.
    const msmtpBin = config.msmtpBin || process.env.SEVIO_EMAIL_MSMTP_BIN || 'msmtp';
    // The rendered, 0600 per-flow msmtprc: path only, never its contents.
    const msmtpConfig = config.msmtpConfig || process.env.SEVIO_EMAIL_MSMTP_CONFIG || '';
    // The sender mailbox. It is a header value, so it is validated here and
    // never taken from a message.
    const fromAddress = config.from || process.env.SEVIO_EMAIL_FROM || '';

    const loggerBin = config.loggerBin || process.env.SEVIO_EMAIL_LOGGER_BIN || 'logger';
    const loggerTag = config.loggerTag || process.env.SEVIO_EMAIL_LOGGER_TAG || 'sevio-email';

    const queueWaitMs = normalizeTimerMs(
      config.queueWaitMs !== undefined ? config.queueWaitMs : process.env.SEVIO_EMAIL_QUEUE_WAIT_MS,
      QUEUE_WAIT_MS
    );
    const slotMaxMs = normalizeTimerMs(
      config.slotMaxMs !== undefined ? config.slotMaxMs : process.env.SEVIO_EMAIL_SLOT_MAX_MS,
      SLOT_MAX_MS
    );
    const perAttemptDeadlineMs = normalizeTimerMs(
      config.perAttemptDeadlineMs !== undefined
        ? config.perAttemptDeadlineMs
        : process.env.SEVIO_EMAIL_PER_ATTEMPT_MS,
      PER_ATTEMPT_DEADLINE_MS
    );
    const backoffMs = normalizeTimerMs(
      config.backoffMs !== undefined ? config.backoffMs : process.env.SEVIO_EMAIL_BACKOFF_MS,
      BACKOFF_MS
    );

    const children = new Set();
    // Attempts waiting out the backoff, with their slot's drop(): on close the
    // timer is cancelled and the slot released at once.
    const retryTimers = new Set();
    let closed = false;

    node.status({ fill: 'blue', shape: 'dot', text: 'ready' });

    node.on('input', function (msg, send, done) {
      send = send || node.send.bind(node);
      done = done || function () {};

      // `release` is the queue slot acquired for this input. release() is
      // idempotent and must be called on every terminal path — success, every
      // failure and the closed early-returns — so a failed or aborted send can
      // never wedge the queue. `slotReleased()` is true once the slot watchdog
      // has freed the slot under this input.
      let release = null;
      let slotReleased = null;

      // The outgoing message keeps the whole incoming msg, so `alertCtx`
      // (and with it `alertCtx.runId`) survives the send and the flow's run
      // tag still correlates — the same way `sevio-sms` preserves it.
      const fail = (code, message, accepted, total) => {
        if (release) release();
        if (closed) return;
        node.status({ fill: 'red', shape: 'dot', text: code });
        node.error(message, msg);
        send([null, Object.assign({}, msg, {
          payload: {
            ok: false,
            code: code,
            message: message,
            accepted: accepted || 0,
            total: total || 0
          }
        })]);
        done();
      };

      const succeed = (accepted) => {
        if (release) release();
        if (closed) return;
        node.status({ fill: 'green', shape: 'dot', text: 'accepted' });
        send([Object.assign({}, msg, { payload: { ok: true, accepted: accepted } }), null]);
        done();
      };

      // A slot held when the node closes is dropped without any output or
      // done(): Node-RED has already closed the node and drops both.
      const drop = () => {
        if (release) release();
      };

      const payload = msg.payload;
      const alertId = payload && payload.alertId;
      const subject = payload && payload.subject;
      const body = payload && payload.body;

      if (typeof alertId !== 'string' || alertId === '') {
        fail('invalid_payload', 'msg.payload must be { alertId, subject, body }');
        return;
      }
      // The subject becomes a header: CR, LF and NUL are refused here as well
      // as at save time, so a stored legacy value can never inject a header
      // even before the RFC 2047 encoding is applied.
      if (typeof subject !== 'string' || subject.trim() === '' || FORBIDDEN_CONTROL.test(subject)) {
        fail('invalid_payload', 'msg.payload.subject must be a non-empty string with no CR, LF or NUL');
        return;
      }
      if (typeof body !== 'string') {
        fail('invalid_payload', 'msg.payload.body must be a string');
        return;
      }
      if (msmtpConfig === '') {
        fail('email_unavailable', 'no msmtp config file configured (msmtpConfig or SEVIO_EMAIL_MSMTP_CONFIG)');
        return;
      }
      const fromProblem = addressProblem(fromAddress);
      if (fromProblem !== null) {
        fail('email_unavailable', `no usable sender address configured (from or SEVIO_EMAIL_FROM): ${fromProblem}`);
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
          // The cap is enforced before any send and never truncates: sending
          // to a silently shortened list would report success for recipients
          // that were never attempted (spec §3.6).
          if (to.length > FANOUT_CAP) {
            fail(
              'too_many_recipients',
              `alert ${alertId} has ${to.length} recipients, more than the fan-out cap of ${FANOUT_CAP}`,
              0,
              to.length
            );
            return;
          }

          // One msmtp invocation per recipient, sequentially, fail-fast.
          // Never a CC/BCC list: each recipient gets its own envelope, so one
          // refusal cannot be reported as a whole-list failure and one
          // acceptance cannot be inferred from another's.
          const sendNext = (index, attempt) => {
            if (closed) {
              drop();
              return;
            }
            // The watchdog already reclaimed the slot for the next alert: an
            // abandoned run must not send to another recipient.
            if (slotReleased && slotReleased()) {
              return;
            }
            if (index >= to.length) {
              succeed(to.length);
              return;
            }

            // `child` may still be null when the callback runs synchronously
            // (a rejected address, or a spawn that throws): both paths return
            // no child, so there is nothing to register or remove.
            let child = null;
            child = invokeMsmtp(msmtpBin, msmtpConfig, fromAddress, to[index], subject, body, perAttemptDeadlineMs, (result) => {
              if (child) children.delete(child);
              if (closed) {
                drop();
                return;
              }
              if (slotReleased && slotReleased()) {
                return;
              }
              if (result.invalidAddress) {
                // A stored address that fails the last check is never handed
                // to the transport: permanent, and no later recipient is
                // attempted (fail-fast).
                fail(
                  'invalid_recipient',
                  `recipient ${index + 1}/${to.length} ${result.invalidAddress}`,
                  index,
                  to.length
                );
                return;
              }

              const outcome = classifyAttempt(result);
              if (result.spawnCode === null && result.exitCode === 0) {
                // Accepted by the relay: the final 250 arrived and msmtp
                // exited cleanly.
                logSend(loggerBin, loggerTag, alertId, index, to.length, attempt, 'ok');
                sendNext(index + 1, 1);
                return;
              }

              logSend(loggerBin, loggerTag, alertId, index, to.length, attempt, outcome.code);
              if (outcome.retry && attempt < ATTEMPTS) {
                const entry = { timer: null, drop: drop };
                entry.timer = setTimeout(() => {
                  retryTimers.delete(entry);
                  sendNext(index, attempt + 1);
                }, backoffMs);
                retryTimers.add(entry);
                return;
              }
              fail(outcome.code, describeFailure(outcome.code, index, to.length, attempt), index, to.length);
            });
            if (child) children.add(child);
          };

          sendNext(0, 1);
        });
      }, () => {
        fail('queue_timeout', `email queue wait exceeded ${queueWaitMs}ms`);
      }, slotMaxMs);
    });

    node.on('close', function (removed, done) {
      closed = true;
      // Cancel this node's queued-but-not-started waiters: a closed node must
      // never start a send.
      for (let i = waiters.length - 1; i >= 0; i -= 1) {
        if (waiters[i].owner === node) {
          clearTimeout(waiters[i].timer);
          waiters.splice(i, 1);
        }
      }
      // A close during the backoff must free the slot now, not when the timer
      // would have fired.
      for (const retry of retryTimers) {
        clearTimeout(retry.timer);
        retry.drop();
      }
      retryTimers.clear();
      // Kill the in-flight child and stop the fan-out: its callback lands on a
      // closed early-return, which releases the slot.
      for (const child of children) {
        child.kill();
      }
      children.clear();
      done();
    });
  }

  RED.nodes.registerType('sevio-email', SevioEmailNode);
};

module.exports.constants = CONSTANTS;
