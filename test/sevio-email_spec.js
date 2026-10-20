const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const registerSevioEmail = require('../sevio-email.js');
const { constants } = require('../sevio-email.js');

function createNodeFactory() {
  const types = {};
  const RED = {
    nodes: {
      createNode(node, config) {
        node.config = config;
        node.name = config.name;
        node.statusCalls = [];
        node.errors = [];
        node.warnings = [];
        node.sent = [];
        node.handlers = {};
        node.status = (update) => node.statusCalls.push(update);
        node.error = (err, msg) => node.errors.push({ err: err, msg: msg });
        node.warn = (warning) => node.warnings.push(warning);
        node.send = (messages) => node.sent.push(messages);
        node.on = (event, handler) => { node.handlers[event] = handler; };
        node.emit = (event, ...args) => node.handlers[event](...args);
      },
      registerType(name, ctor) {
        types[name] = ctor;
      }
    }
  };
  registerSevioEmail(RED);
  return (config) => new types['sevio-email'](config);
}

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'sevio-email-'));
}

function writeRecipients(dir, contents) {
  const file = path.join(dir, 'recipients.json');
  fs.writeFileSync(file, typeof contents === 'string' ? contents : JSON.stringify(contents));
  return file;
}

/**
 * Fake `msmtp`. Records every invocation as a JSON line in <dir>/calls.log
 * (argv included), reads the composed message from stdin into
 * <dir>/messages/<n>.eml, then replays <dir>/scenario.json — one step per
 * invocation, the last step repeating. A step is { exit, stderr } for an
 * immediate result, { delayMs } to linger (with stdin already read), or
 * { hang } to stay alive without reading stdin, so the test can observe the
 * kill-on-close path (it marks <dir>/killed when SIGTERMed).
 */
const FAKE_MSMTP_SOURCE = `#!/usr/bin/env node
'use strict';
const fs = require('fs');
const path = require('path');

const dir = __dirname;
const args = process.argv.slice(2);
fs.mkdirSync(path.join(dir, 'messages'), { recursive: true });
fs.appendFileSync(path.join(dir, 'calls.log'), JSON.stringify({ argv: args }) + '\\n');

const count = fs.readFileSync(path.join(dir, 'calls.log'), 'utf8').trim().split('\\n').filter(Boolean).length;
const scenario = JSON.parse(fs.readFileSync(path.join(dir, 'scenario.json'), 'utf8'));
const step = scenario.steps[Math.min(count - 1, scenario.steps.length - 1)];

process.on('SIGTERM', () => {
  fs.writeFileSync(path.join(dir, 'killed'), '1');
  process.exit(143);
});

let input = '';
process.stdin.setEncoding('utf8');
const emit = () => {
  fs.writeFileSync(path.join(dir, 'messages', count + '.eml'), input);
  if (step.stderr !== undefined) process.stderr.write(step.stderr);
  process.exitCode = step.exit || 0;
};

if (step.hang) {
  fs.writeFileSync(path.join(dir, 'pid'), String(process.pid));
  setInterval(() => {}, 1000);
} else {
  process.stdin.on('data', (chunk) => { input += chunk; });
  process.stdin.on('end', () => {
    if (step.delayMs) {
      fs.writeFileSync(path.join(dir, 'pid'), String(process.pid));
      setTimeout(emit, step.delayMs);
    } else {
      emit();
    }
  });
}
`;

function writeFakeMsmtp(dir, steps, name = 'fake-msmtp') {
  const bin = path.join(dir, name);
  fs.writeFileSync(bin, FAKE_MSMTP_SOURCE);
  fs.chmodSync(bin, 0o755);
  fs.writeFileSync(path.join(dir, 'scenario.json'), JSON.stringify({ steps: steps }));
  return bin;
}

function readCalls(dir) {
  const file = path.join(dir, 'calls.log');
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function readMessage(dir, n) {
  return fs.readFileSync(path.join(dir, 'messages', `${n}.eml`), 'utf8');
}

function sendInput(node, payload) {
  return new Promise((resolve) => {
    node.emit('input', { payload: payload }, null, (err) => resolve(err));
  });
}

// For messages that carry siblings of payload (alertCtx, _msgid, …).
function sendFullMsg(node, msg) {
  return new Promise((resolve) => {
    node.emit('input', msg, null, (err) => resolve(err));
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function waitFor(predicate, timeoutMs = 2000) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      if (predicate()) {
        resolve();
      } else if (Date.now() - started > timeoutMs) {
        reject(new Error('waitFor timed out'));
      } else {
        setTimeout(tick, 5);
      }
    };
    tick();
  });
}

// ---------------------------------------------------------------------------
// Fake `logger` binary: appends its argv as a JSON line to <dir>/logger.log.
// ---------------------------------------------------------------------------
const FAKE_LOGGER_SOURCE = `#!/usr/bin/env node
'use strict';
const fs = require('fs');
const path = require('path');
fs.appendFileSync(path.join(__dirname, 'logger.log'), JSON.stringify(process.argv.slice(2)) + '\\n');
`;

function writeFakeLogger(dir, name = 'fake-logger') {
  const bin = path.join(dir, name);
  fs.writeFileSync(bin, FAKE_LOGGER_SOURCE);
  fs.chmodSync(bin, 0o755);
  return bin;
}

function logLines(dir) {
  const file = path.join(dir, 'logger.log');
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map((line) => {
    const argv = JSON.parse(line);
    return argv[argv.length - 1];
  });
}

// Keep the suite off the host syslog: nodes resolve this stub via
// SEVIO_EMAIL_LOGGER_BIN unless a test passes its own loggerBin.
process.env.SEVIO_EMAIL_LOGGER_BIN = writeFakeLogger(tempDir());

// The response to every successful send: accepted by the relay.
const ACCEPTED = { exit: 0 };
// smtp reply shapes msmtp prints as `msmtp: server message: <code> <text>`.
const DEFERRED_451 = { exit: 75, stderr: 'msmtp: server message: 451 4.3.0 try again later\n' };
const REJECTED_550 = { exit: 69, stderr: 'msmtp: server message: 550 5.1.1 no such user\n' };
// The whole message reached the child, then the final 250 was lost: msmtp
// exits EX_TEMPFAIL with no server reply at all.
const LOST_FINAL_250 = { exit: 75 };

function makeNode(dir, config) {
  const merged = Object.assign({
    name: 'test',
    msmtpConfig: path.join(dir, 'flow.msmtprc'),
    from: 'alerts@example.com',
    backoffMs: 10
  }, config);
  if (!merged.recipientsFile) {
    merged.recipientsFile = writeRecipients(dir, {
      'a-1': { channel: 'email', targets: ['ops@example.com'] }
    });
  }
  return createNodeFactory()(merged);
}

test('registers the sevio-email node type', () => {
  const makeNode = createNodeFactory();
  assert.equal(typeof makeNode, 'function');
  const node = makeNode({ name: 'test' });
  assert.equal(typeof node.on, 'function');
});

// ---------------------------------------------------------------------------
// The §3.6 arithmetic: the constants are the published numbers, and the slot
// bound is derived from them — TERMINATION_GRACE is counted once, in the slot
// bound, and nowhere else.
// ---------------------------------------------------------------------------
test('timing: the slot bound and the sending deadline follow §3.6 arithmetic', () => {
  assert.equal(constants.PER_ATTEMPT_DEADLINE_MS, 30000);
  assert.equal(constants.ATTEMPTS, 2);
  assert.equal(constants.BACKOFF_MS, 2000);
  assert.equal(constants.FANOUT_CAP, 5);
  assert.equal(constants.QUEUE_WAIT_MS, 30000);
  assert.equal(constants.TERMINATION_GRACE_MS, 5000);
  assert.equal(constants.MARGIN_MS, 30000);

  const perRecipient = constants.ATTEMPTS * constants.PER_ATTEMPT_DEADLINE_MS +
    (constants.ATTEMPTS - 1) * constants.BACKOFF_MS;
  assert.equal(constants.PER_RECIPIENT_MS, 62000);
  assert.equal(perRecipient, 62000);

  assert.equal(constants.SLOT_MAX_MS, 315000);
  assert.equal(constants.SLOT_MAX_MS, constants.FANOUT_CAP * perRecipient + constants.TERMINATION_GRACE_MS);
  assert.equal(constants.SENDING_DEADLINE_MS, 375000);
  assert.equal(
    constants.SENDING_DEADLINE_MS,
    constants.QUEUE_WAIT_MS + constants.SLOT_MAX_MS + constants.MARGIN_MS
  );
});

test('invalid payload goes to the failure output only', async () => {
  const dir = tempDir();
  const node = makeNode(dir);

  await sendInput(node, { alertId: 'a-1', body: 'no subject' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'invalid_payload');
  assert.equal(failure.payload.accepted, 0);
  assert.equal(failure.payload.total, 0);
  assert.equal(node.errors.length, 1);
  assert.equal(readCalls(dir).length, 0);
});

test('a subject carrying CR/LF is refused before any send', async () => {
  const dir = tempDir();
  const node = makeNode(dir);

  await sendInput(node, { alertId: 'a-1', subject: 'Hi\r\nBcc: evil@example.com', body: 'x' });

  assert.equal(node.sent[0][1].payload.code, 'invalid_payload');
  assert.equal(readCalls(dir).length, 0);
});

// ---------------------------------------------------------------------------
// Recipients artefact: the email reader's side of §3.3
// ---------------------------------------------------------------------------

test('artefact: a legacy bare array is the sms channel and fails with channel_mismatch', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['ops@example.com'] });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'channel_mismatch');
  assert.match(failure.payload.message, /legacy array/);
  assert.equal(failure.payload.accepted, 0);
  assert.equal(failure.payload.total, 0);
  // The address that must not be mailed is not echoed into the failure either.
  assert.equal(failure.payload.message.includes('ops@example.com'), false);
  assert.equal(readCalls(dir).length, 0, 'a channel mismatch must never reach the transport');
});

test('artefact: a sms entry fails closed with channel_mismatch', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'sms', targets: ['+390000000000'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(node.sent[0][1].payload.code, 'channel_mismatch');
  assert.equal(readCalls(dir).length, 0);
});

test('artefact: an unknown or missing channel fails closed with channel_mismatch', async (t) => {
  for (const entry of [{ channel: 'webhook', targets: ['ops@example.com'] }, { targets: ['ops@example.com'] }]) {
    const dir = tempDir();
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const recipientsFile = writeRecipients(dir, { 'a-1': entry });
    const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

    const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
    await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

    assert.equal(node.sent[0][1].payload.code, 'channel_mismatch');
    assert.equal(readCalls(dir).length, 0);
  }
});

test('artefact: a non-object entry fails closed with channel_mismatch', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': 'ops@example.com' });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(node.sent[0][1].payload.code, 'channel_mismatch');
  assert.equal(readCalls(dir).length, 0);
});

test('artefact: an absent entry fails with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'other-alert': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(node.sent[0][1].payload.code, 'no_recipients');
  assert.equal(readCalls(dir).length, 0);
});

test('artefact: a missing, empty or non-array targets list fails with no_recipients', async (t) => {
  for (const entry of [{ channel: 'email' }, { channel: 'email', targets: [] }, { channel: 'email', targets: 'ops@example.com' }]) {
    const dir = tempDir();
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const recipientsFile = writeRecipients(dir, { 'a-1': entry });
    const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

    const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
    await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

    const [success, failure] = node.sent[0];
    assert.equal(success, null);
    // A targets defect on the email channel is never a channel mismatch.
    assert.equal(failure.payload.code, 'no_recipients');
    assert.equal(failure.payload.message, 'alert a-1 has no usable targets list');
    assert.equal(readCalls(dir).length, 0);
  }
});

test('artefact: an unreadable or invalid file fails with recipients_unavailable', async (t) => {
  const cases = [
    { name: 'missing', contents: null },
    { name: 'invalid JSON', contents: '{not json' },
    { name: 'array top level', contents: [] }
  ];
  for (const testCase of cases) {
    const dir = tempDir();
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const recipientsFile = testCase.contents === null
      ? path.join(dir, 'missing.json')
      : writeRecipients(dir, testCase.contents);
    const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

    const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
    await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

    assert.equal(node.sent[0][1].payload.code, 'recipients_unavailable', testCase.name);
    assert.equal(readCalls(dir).length, 0);
  }
});

test('artefact: the file is read at send time inside the slot', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    hold: { channel: 'email', targets: ['first@example.com'] },
    'a-1': { channel: 'email', targets: ['before@example.com'] }
  });
  // The holder lingers 150 ms per call; the waiter's turn comes afterwards.
  const msmtpBin = writeFakeMsmtp(dir, [{ delayMs: 150, exit: 0 }]);
  const make = createNodeFactory();
  const holder = make({
    name: 'holder',
    recipientsFile: recipientsFile,
    msmtpBin: msmtpBin,
    msmtpConfig: path.join(dir, 'flow.msmtprc'),
    from: 'alerts@example.com'
  });
  const waiter = make({
    name: 'waiter',
    recipientsFile: recipientsFile,
    msmtpBin: msmtpBin,
    msmtpConfig: path.join(dir, 'flow.msmtprc'),
    from: 'alerts@example.com'
  });

  const holdPromise = sendInput(holder, { alertId: 'hold', subject: 'Hi', body: 'hold' });
  await waitFor(() => readCalls(dir).length === 1);
  const waitPromise = sendInput(waiter, { alertId: 'a-1', subject: 'Hi', body: 'fresh' });
  // The waiter is queued now: rewrite the file before its turn comes.
  fs.writeFileSync(recipientsFile, JSON.stringify({
    hold: { channel: 'email', targets: ['first@example.com'] },
    'a-1': { channel: 'email', targets: ['after@example.com'] }
  }));

  await Promise.all([holdPromise, waitPromise]);
  const calls = readCalls(dir);
  assert.equal(calls.length, 2);
  assert.equal(calls[1].argv[2], 'after@example.com');
});

// ---------------------------------------------------------------------------
// One msmtp invocation per recipient, argv asserted
// ---------------------------------------------------------------------------

test('sends with `--file=<config> -- <address>` and reports accepted', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);
  const configFile = path.join(dir, 'flow.msmtprc');

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin, msmtpConfig: configFile });
  const err = await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'hello' });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].argv, [`--file=${configFile}`, '--', 'ops@example.com']);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, accepted: 1 });
  assert.equal(node.statusCalls[node.statusCalls.length - 1].text, 'accepted');
});

test('two recipients get two separate invocations and separate envelopes', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['first@example.com', 'second@example.com'];
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: recipients }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);
  const configFile = path.join(dir, 'flow.msmtprc');

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin, msmtpConfig: configFile });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'body' });

  const calls = readCalls(dir);
  assert.equal(calls.length, 2, 'one invocation per recipient, never a CC/BCC list');
  assert.deepEqual(calls.map((call) => call.argv), recipients.map((address) => [
    `--file=${configFile}`, '--', address
  ]));
  assert.deepEqual(calls.map((call) => call.argv[2]), recipients);
  // Neither message names the other recipient: separate envelopes, not a list.
  assert.equal(readMessage(dir, 1).includes(recipients[1]), false);
  assert.equal(readMessage(dir, 2).includes(recipients[0]), false);
  assert.match(readMessage(dir, 1), /^To: first@example\.com\r$/m);
  assert.match(readMessage(dir, 2), /^To: second@example\.com\r$/m);
  assert.deepEqual(node.sent[0][0].payload, { ok: true, accepted: 2 });
});

test('a hyphen-leading address is refused and `--` is always present', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);
  const configFile = path.join(dir, 'flow.msmtprc');

  // 1. A valid send proves the option terminator is in the argv.
  const okFile = writeRecipients(dir, {
    ok: { channel: 'email', targets: ['ops@example.com'] }
  });
  const okNode = makeNode(dir, { recipientsFile: okFile, msmtpBin: msmtpBin, msmtpConfig: configFile });
  await sendInput(okNode, { alertId: 'ok', subject: 'Hi', body: 'x' });
  const okCall = readCalls(dir)[0];
  assert.equal(okCall.argv.includes('--'), true);
  assert.deepEqual(okCall.argv, [`--file=${configFile}`, '--', 'ops@example.com']);

  // 2. A leading hyphen is refused before any invocation, so it can never be
  // parsed as an option.
  const hyphenFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['-option@example.com'] }
  });
  const hyphenNode = makeNode(dir, { recipientsFile: hyphenFile, msmtpBin: msmtpBin, msmtpConfig: configFile });
  await sendInput(hyphenNode, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(readCalls(dir).length, 1, 'the hyphen-leading address never reaches msmtp');
  const [success, failure] = hyphenNode.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'invalid_recipient');
  assert.match(failure.payload.message, /must not begin with "-"/);
  // The refused address is not echoed into the failure.
  assert.equal(failure.payload.message.includes('-option@example.com'), false);
});

test('an address with a display name is refused before any invocation', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['Ops <ops@example.com>'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(readCalls(dir).length, 0);
  assert.equal(node.sent[0][1].payload.code, 'invalid_recipient');
});

// ---------------------------------------------------------------------------
// MIME composition
// ---------------------------------------------------------------------------

function parseMessage(raw) {
  const separator = raw.indexOf('\r\n\r\n');
  assert.notEqual(separator, -1, 'the message has a header/body separator');
  const headerBlock = raw.slice(0, separator);
  // Unfold continuation lines, then split into name/value pairs.
  const unfolded = headerBlock.replace(/\r\n[ \t]/g, ' ');
  const headers = new Map();
  for (const line of unfolded.split('\r\n')) {
    const colon = line.indexOf(':');
    assert.notEqual(colon, -1, `a header line is well-formed: ${JSON.stringify(line)}`);
    headers.set(line.slice(0, colon), line.slice(colon + 1).trim());
  }
  return { headerBlock: headerBlock, headers: headers, body: raw.slice(separator + 4) };
}

function decodeEncodedWords(value) {
  const words = value.match(/=\?UTF-8\?B\?[^?]*\?=/g) || [];
  return words.map((word) => {
    const base64 = word.slice('=?UTF-8?B?'.length, -2);
    return Buffer.from(base64, 'base64').toString('utf8');
  }).join('');
}

test('composes a well-formed MIME message with the exact UTF-8 body', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);
  const body = 'Livello € carburante basso\nTrattore 12 è fermo — 🚜\nsecond line';

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Low fuel', body: body });

  const raw = readMessage(dir, 1);
  const message = parseMessage(raw);
  assert.equal(message.headers.get('From'), 'alerts@example.com');
  assert.equal(message.headers.get('To'), 'ops@example.com');
  assert.equal(message.headers.get('Subject'), 'Low fuel');
  assert.match(message.headers.get('Date'), /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} [+-]\d{4}$/);
  assert.equal(message.headers.get('MIME-Version'), '1.0');
  assert.equal(message.headers.get('Content-Type'), 'text/plain; charset=UTF-8');
  assert.equal(message.headers.get('Content-Transfer-Encoding'), 'base64');

  // Every body line is a short Base64 line — no raw non-ASCII in the body.
  for (const line of message.body.split('\r\n').filter(Boolean)) {
    assert.ok(line.length <= 76, `base64 line is at most 76 chars, got ${line.length}`);
    assert.match(line, /^[A-Za-z0-9+/=]+$/);
  }
  const decoded = Buffer.from(message.body.replace(/\r\n/g, ''), 'base64').toString('utf8');
  assert.equal(decoded, body, 'the base64 body decodes to the exact UTF-8 text');
  assert.equal(decoded.startsWith('Livello € carburante basso\nTrattore 12 è fermo'), true);

  // The whole message is CRLF-terminated: no bare LF outside the body text
  // (which only exists after Base64, so the wire form has none at all).
  assert.equal(raw.replace(/\r\n/g, '').includes('\n'), false);
});

test('a non-ASCII subject is RFC 2047 Base64-encoded and decodes back', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);
  const subject = 'Livello € basso — trattore 12 è fermo, 🚜 nel campo';

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: subject, body: 'x' });

  const raw = readMessage(dir, 1);
  const message = parseMessage(raw);
  const header = message.headers.get('Subject');
  assert.match(header, /^=\?UTF-8\?B\?/);
  assert.equal(header.includes('Livello'), false, 'no raw subject text in the header');
  assert.equal(decodeEncodedWords(header), subject);
  // The header block itself is pure ASCII, so no non-ASCII byte can leak into
  // a header.
  assert.equal(/[^\x00-\x7f]/.test(message.headerBlock), false);
  // Each encoded word is within RFC 2047's 75-character limit.
  for (const word of header.match(/=\?UTF-8\?B\?[^?]*\?=/g) || []) {
    assert.ok(word.length <= 75, `encoded word is at most 75 chars, got ${word.length}`);
  }
});

test('an ASCII subject is written as-is, never encoded', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Pump 3 offline', body: 'x' });

  const message = parseMessage(readMessage(dir, 1));
  assert.equal(message.headers.get('Subject'), 'Pump 3 offline');
});

test('the binary and the config path never come from a message', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);
  const configFile = path.join(dir, 'flow.msmtprc');

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin, msmtpConfig: configFile });
  await sendInput(node, {
    alertId: 'a-1',
    subject: 'Hi',
    body: 'x',
    // Ignored: only node configuration or the environment names the binary
    // and the credential file.
    msmtpBin: path.join(dir, 'evil'),
    msmtpConfig: path.join(dir, 'evil.msmtprc'),
    from: 'evil@example.com'
  });

  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].argv, [`--file=${configFile}`, '--', 'ops@example.com']);
  // Nor can a message choose the From header.
  assert.match(readMessage(dir, 1), /^From: alerts@example\.com\r$/m);
});

// ---------------------------------------------------------------------------
// Failure classification and retries (§3.5, §3.6)
// ---------------------------------------------------------------------------

test('an explicit 4xx deferral is retried and can succeed', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [DEFERRED_451, ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(readCalls(dir).length, 2, 'a 4xx is not an acceptance: retried once');
  assert.deepEqual(node.sent[0][0].payload, { ok: true, accepted: 1 });
});

test('a 4xx on both attempts fails with deferred and the accepted count', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [DEFERRED_451]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(readCalls(dir).length, 2);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, {
    ok: false,
    code: 'deferred',
    message: 'recipient 1/1 was not accepted by the relay: deferred after 2 attempt(s)',
    accepted: 0,
    total: 1
  });
});

test('a 5xx is permanent and is not retried', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [REJECTED_550]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  await delay(60); // longer than the 10 ms backoff
  assert.equal(readCalls(dir).length, 1, 'a 5xx must be attempted exactly once');
  assert.equal(node.sent[0][1].payload.code, 'rejected');
});

test('a lost final 250 is outcome_unknown and is never retried', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  // The whole message was consumed, then msmtp exited EX_TEMPFAIL with no
  // server reply. A generic temporary exit must not be read as retry-safe.
  const msmtpBin = writeFakeMsmtp(dir, [LOST_FINAL_250]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  await delay(80); // longer than the 10 ms backoff: a retry would have fired
  assert.equal(readCalls(dir).length, 1, 'a lost 250 must never be retried: it can duplicate mail');
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'outcome_unknown');
  assert.equal(failure.payload.accepted, 0);
  assert.equal(failure.payload.total, 1);
  assert.match(failure.payload.message, /outcome unknown/);
});

test('a connection refusal is retried even though the message was buffered to the child', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const connectionRefused = {
    exit: 75,
    stderr: 'msmtp: cannot connect to smtp.example.com, port 587: Connection refused\n'
  };
  const msmtpBin = writeFakeMsmtp(dir, [connectionRefused, ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(readCalls(dir).length, 2, 'a connection failure is definitively not accepted: retried');
  assert.deepEqual(node.sent[0][0].payload, { ok: true, accepted: 1 });
});

test('an authentication failure is permanent and is not retried', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [
    { exit: 77, stderr: 'msmtp: authentication failed (method PLAIN)\n' }
  ]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  await delay(60);
  assert.equal(readCalls(dir).length, 1);
  assert.equal(node.sent[0][1].payload.code, 'rejected');
});

test('a missing msmtp binary fails with email_unavailable', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });

  const node = makeNode(dir, {
    recipientsFile: recipientsFile,
    msmtpBin: path.join(dir, 'no-such-msmtp')
  });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'email_unavailable');
  assert.equal(failure.payload.accepted, 0);
  assert.equal(failure.payload.total, 1);
});

test('a hung child is killed at the per-attempt deadline and reports outcome_unknown', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  // The child never answers, but the whole message already reached its stdin:
  // the terminating dot may have been written, so a retry could duplicate.
  const msmtpBin = writeFakeMsmtp(dir, [{ hang: true }]);

  const node = makeNode(dir, {
    recipientsFile: recipientsFile,
    msmtpBin: msmtpBin,
    perAttemptDeadlineMs: 150
  });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  await delay(80); // longer than the 10 ms backoff: a retry would have fired
  assert.equal(readCalls(dir).length, 1, 'a timeout after the message data must not be retried');
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'outcome_unknown');
  assert.equal(failure.payload.total, 1);

  // SIGKILL: the fake msmtp cannot catch it, so its SIGTERM marker is absent
  // and the pid is gone.
  assert.equal(fs.existsSync(path.join(dir, 'killed')), false, 'the deadline kill is SIGKILL');
  const pid = Number(fs.readFileSync(path.join(dir, 'pid'), 'utf8'));
  await waitFor(() => {
    try {
      process.kill(pid, 0);
      return false;
    } catch (killErr) {
      return killErr.code === 'ESRCH';
    }
  });
});

test('fail-fast: a failure on the second recipient reports accepted 1/2 and stops', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['first@example.com', 'second@example.com', 'third@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED, REJECTED_550, ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });
  await delay(50);

  const calls = readCalls(dir);
  assert.equal(calls.length, 2, 'the third recipient is never attempted after a failure');
  assert.deepEqual(calls.map((call) => call.argv[2]), ['first@example.com', 'second@example.com']);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'rejected');
  assert.equal(failure.payload.accepted, 1);
  assert.equal(failure.payload.total, 3);
});

test('the fan-out cap refuses an oversized list instead of truncating it', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['a', 'b', 'c', 'd', 'e', 'f'].map((name) => `${name}@example.com`);
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: recipients }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(readCalls(dir).length, 0, 'the cap is enforced before any send');
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'too_many_recipients');
  assert.equal(failure.payload.accepted, 0);
  assert.equal(failure.payload.total, 6);
});

test('exactly the fan-out cap is allowed', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['a', 'b', 'c', 'd', 'e'].map((name) => `${name}@example.com`);
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: recipients }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  assert.equal(readCalls(dir).length, 5);
  assert.deepEqual(node.sent[0][0].payload, { ok: true, accepted: 5 });
});

// ---------------------------------------------------------------------------
// alertCtx, logging, close, queue
// ---------------------------------------------------------------------------

test('alertCtx — and its runId — survives both outputs', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    ok: { channel: 'email', targets: ['ops@example.com'] },
    bad: { channel: 'email', targets: ['ops@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED, REJECTED_550]);

  const okNode = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendFullMsg(okNode, {
    payload: { alertId: 'ok', subject: 'Hi', body: 'x' },
    alertCtx: { runId: '1729410000000.7', alertId: 'ok', triggerNumber: 1 }
  });
  assert.equal(okNode.sent[0][0].alertCtx.runId, '1729410000000.7');

  const badNode = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  await sendFullMsg(badNode, {
    payload: { alertId: 'bad', subject: 'Hi', body: 'x' },
    alertCtx: { runId: '1729410000001.8', alertId: 'bad', triggerNumber: 2 }
  });
  assert.equal(badNode.sent[0][1].alertCtx.runId, '1729410000001.8');
});

test('logging names recipients by position and never contains an address', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['first@example.com', 'second@example.com'];
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: recipients }
  });
  const msmtpBin = writeFakeMsmtp(dir, [ACCEPTED, REJECTED_550]);
  const loggerBin = writeFakeLogger(dir);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin, loggerBin: loggerBin });
  await sendInput(node, { alertId: 'a-1', subject: 'Hi', body: 'x' });

  await waitFor(() => logLines(dir).length === 2);
  assert.deepEqual(logLines(dir), [
    'alert=a-1 to=1/2 attempt=1/2 outcome=ok',
    'alert=a-1 to=2/2 attempt=1/2 outcome=rejected'
  ]);
  const raw = fs.readFileSync(path.join(dir, 'logger.log'), 'utf8');
  for (const address of recipients) {
    assert.equal(raw.includes(address), false, `an address leaked into the log: ${raw}`);
    assert.equal(raw.includes(address.split('@')[0]), false, 'not even the local part is logged');
  }
  // msmtp's stderr carries the address and protocol dialogue: never logged.
  assert.equal(raw.includes('550'), false);
  assert.equal(raw.includes('server message'), false);
});

test('close kills the in-flight child and stops later recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['first@example.com', 'second@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [{ hang: true }]);

  const node = makeNode(dir, { recipientsFile: recipientsFile, msmtpBin: msmtpBin });
  node.emit('input', { payload: { alertId: 'a-1', subject: 'Hi', body: 'x' } }, null, () => {});
  await waitFor(() => fs.existsSync(path.join(dir, 'pid')));

  node.emit('close', false, () => {});
  await delay(150);

  assert.equal(readCalls(dir).length, 1, 'no later recipient is attempted after close');
  assert.equal(fs.existsSync(path.join(dir, 'killed')), true, 'the in-flight child received SIGTERM');
  assert.equal(node.sent.length, 0);
  assert.equal(node.errors.length, 0);
  assert.equal(node.statusCalls.some((status) => status.text === 'accepted'), false);
});

test('close while queued cancels the waiter without sending', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    hold: { channel: 'email', targets: ['hold@example.com'] },
    'a-1': { channel: 'email', targets: ['queued@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [{ delayMs: 200, exit: 0 }]);
  const make = createNodeFactory();
  const baseConfig = {
    recipientsFile: recipientsFile,
    msmtpBin: msmtpBin,
    msmtpConfig: path.join(dir, 'flow.msmtprc'),
    from: 'alerts@example.com'
  };
  const holder = make(Object.assign({ name: 'holder' }, baseConfig));
  const queued = make(Object.assign({ name: 'queued' }, baseConfig));

  const holdPromise = sendInput(holder, { alertId: 'hold', subject: 'Hi', body: 'hold' });
  await waitFor(() => readCalls(dir).length === 1);
  queued.emit('input', { payload: { alertId: 'a-1', subject: 'Hi', body: 'queued' } }, null, () => {});
  queued.emit('close', false, () => {});

  await holdPromise;
  await delay(50);

  assert.equal(queued.sent.length, 0);
  assert.equal(queued.errors.length, 0);
  assert.deepEqual(readCalls(dir).map((call) => call.argv[2]), ['hold@example.com']);
});

test('queue: two email nodes serialise their fan-outs', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['a1@example.com', 'a2@example.com'] },
    'b-1': { channel: 'email', targets: ['b1@example.com'] }
  });
  // Every call lingers: had the two nodes run concurrently, B's call would be
  // logged while A's fan-out is still waiting on its first recipient.
  const msmtpBin = writeFakeMsmtp(dir, [{ delayMs: 100, exit: 0 }]);
  const make = createNodeFactory();
  const baseConfig = {
    recipientsFile: recipientsFile,
    msmtpBin: msmtpBin,
    msmtpConfig: path.join(dir, 'flow.msmtprc'),
    from: 'alerts@example.com'
  };
  const nodeA = make(Object.assign({ name: 'a' }, baseConfig));
  const nodeB = make(Object.assign({ name: 'b' }, baseConfig));

  const [errA, errB] = await Promise.all([
    sendInput(nodeA, { alertId: 'a-1', subject: 'Hi', body: 'first' }),
    sendInput(nodeB, { alertId: 'b-1', subject: 'Hi', body: 'second' })
  ]);

  assert.equal(errA, undefined);
  assert.equal(errB, undefined);
  assert.deepEqual(readCalls(dir).map((call) => call.argv[2]), [
    'a1@example.com', 'a2@example.com', 'b1@example.com'
  ]);
  assert.equal(nodeA.sent[0][0].payload.ok, true);
  assert.equal(nodeB.sent[0][0].payload.ok, true);
});

test('queue: waiting past queueWaitMs fails with queue_timeout and makes no call', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    hold: { channel: 'email', targets: ['hold@example.com'] },
    'a-1': { channel: 'email', targets: ['late@example.com'] }
  });
  const msmtpBin = writeFakeMsmtp(dir, [{ delayMs: 300, exit: 0 }]);
  const make = createNodeFactory();
  const baseConfig = {
    recipientsFile: recipientsFile,
    msmtpBin: msmtpBin,
    msmtpConfig: path.join(dir, 'flow.msmtprc'),
    from: 'alerts@example.com'
  };
  const holder = make(Object.assign({ name: 'holder' }, baseConfig));
  const waiter = make(Object.assign({ name: 'waiter' }, baseConfig, { queueWaitMs: 80 }));

  const holdPromise = sendInput(holder, { alertId: 'hold', subject: 'Hi', body: 'hold' });
  await waitFor(() => readCalls(dir).length === 1);

  await sendInput(waiter, { alertId: 'a-1', subject: 'Hi', body: 'late' });
  const [success, failure] = waiter.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'queue_timeout');
  assert.equal(failure.payload.accepted, 0);
  assert.equal(failure.payload.total, 0);

  await holdPromise;
  assert.deepEqual(readCalls(dir).map((call) => call.argv[2]), ['hold@example.com']);
});
