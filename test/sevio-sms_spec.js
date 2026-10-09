const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const registerSevioSms = require('../sevio-sms.js');

function createNodeFactory() {
  const types = {};
  const RED = {
    nodes: {
      // Mirrors RED.nodes.createNode: attaches the node API to the instance.
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
  registerSevioSms(RED);
  return (config) => new types['sevio-sms'](config);
}

function tempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'sevio-sms-'));
}

function writeRecipients(dir, contents) {
  const file = path.join(dir, 'recipients.json');
  fs.writeFileSync(file, typeof contents === 'string' ? contents : JSON.stringify(contents));
  return file;
}

/**
 * Fake `ubus` CLI. Records every invocation (flags included) as a JSON line in
 * <dir>/calls.log and replays <dir>/scenario.json — one step per invocation,
 * the last step repeats. A step is { exit, stdout, stderr } for an immediate
 * result, or { delayMs } / { hang } to stay alive so the test can observe the
 * kill-on-close path (it marks <dir>/killed when SIGTERMed).
 */
const FAKE_UBUS_SOURCE = `#!/usr/bin/env node
'use strict';
const fs = require('fs');
const path = require('path');

const dir = __dirname;
const args = process.argv.slice(2);

let timeout = null;
let socket = null;
let i = 0;
while (i < args.length && args[i].startsWith('-')) {
  if (args[i] === '-t') {
    timeout = args[i + 1];
    i += 2;
  } else if (args[i] === '-s') {
    socket = args[i + 1];
    i += 2;
  } else {
    i += 1;
  }
}

const rest = args.slice(i);
const call = {
  argv: args,
  timeout: timeout,
  socket: socket,
  verb: rest[0],
  object: rest[1],
  method: rest[2],
  data: rest.length > 3 ? JSON.parse(rest[3]) : null
};
fs.appendFileSync(path.join(dir, 'calls.log'), JSON.stringify(call) + '\\n');

process.on('SIGTERM', () => {
  fs.writeFileSync(path.join(dir, 'killed'), '1');
  process.exit(143);
});

const scenario = JSON.parse(fs.readFileSync(path.join(dir, 'scenario.json'), 'utf8'));
const calls = fs.readFileSync(path.join(dir, 'calls.log'), 'utf8').trim().split('\\n');
const step = scenario.steps[Math.min(calls.length - 1, scenario.steps.length - 1)];

const emit = () => {
  if (step.stdout !== undefined) process.stdout.write(JSON.stringify(step.stdout));
  if (step.stderr !== undefined) process.stderr.write(step.stderr);
  process.exitCode = step.exit || 0;
};

if (step.hang) {
  fs.writeFileSync(path.join(dir, 'pid'), String(process.pid));
  setInterval(() => {}, 1000);
} else if (step.delayMs) {
  fs.writeFileSync(path.join(dir, 'pid'), String(process.pid));
  setTimeout(emit, step.delayMs);
} else {
  emit();
}
`;

function writeFakeUbus(dir, steps, name = 'fake-ubus') {
  const bin = path.join(dir, name);
  fs.writeFileSync(bin, FAKE_UBUS_SOURCE);
  fs.chmodSync(bin, 0o755);
  fs.writeFileSync(path.join(dir, 'scenario.json'), JSON.stringify({ steps: steps }));
  return bin;
}

function readCalls(dir) {
  const file = path.join(dir, 'calls.log');
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function sendInput(node, payload) {
  return new Promise((resolve) => {
    node.emit('input', { payload: payload }, null, (err) => resolve(err));
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

/**
 * Fake `logger` binary (busybox stand-in). Appends its argv as a JSON line to
 * <dir>/logger.log so the test can assert the exact syslog invocation
 * (['-t', <tag>, <line>]).
 */
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

function readLogLines(dir) {
  const file = path.join(dir, 'logger.log');
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

// The logger is invoked as ['-t', <tag>, <line>]: the syslog line is last.
function logLines(dir) {
  return readLogLines(dir).map((argv) => argv[argv.length - 1]);
}

function assertNoMsisdn(dir, numbers) {
  for (const argv of readLogLines(dir)) {
    for (const number of numbers) {
      assert.equal(
        argv.join(' ').includes(number),
        false,
        `MSISDN leaked into logger line: ${argv.join(' ')}`
      );
    }
  }
}

// Keep the suite off the host syslog: nodes resolve this stub via
// SEVIO_SMS_LOGGER_BIN unless a test passes its own loggerBin.
process.env.SEVIO_SMS_LOGGER_BIN = writeFakeLogger(tempDir());

test('registers the sevio-sms node type', () => {
  const makeNode = createNodeFactory();
  assert.equal(typeof makeNode, 'function');
  const node = makeNode({ name: 'test' });
  assert.equal(typeof node.on, 'function');
});

test('invalid payload goes to the failure output only', async () => {
  const node = createNodeFactory()({ name: 'test' });

  let doneCalled = false;
  node.emit('input', { payload: { body: 'hello' } }, null, () => { doneCalled = true; });

  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'invalid_payload');
  assert.equal(typeof failure.payload.message, 'string');
  assert.equal(node.errors.length, 1);
  assert.equal(doneCalled, true);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('non-string body goes to the failure output only', () => {
  const node = createNodeFactory()({ name: 'test' });

  node.emit('input', { payload: { alertId: 'a-1', body: 42 } }, null, () => {});

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'invalid_payload');
});

test('invokes `ubus -t 30 call sms send <payload JSON>` and parses the reply', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].argv, [
    '-t', '30', 'call', 'sms', 'send', JSON.stringify({ to: ['+390000000000'], body: 'hello' })
  ]);
  assert.equal(calls[0].timeout, '30');
  assert.equal(calls[0].verb, 'call');
  assert.equal(calls[0].object, 'sms');
  assert.equal(calls[0].method, 'send');
  assert.deepEqual(calls[0].data, { to: ['+390000000000'], body: 'hello' });
  assert.equal(calls[0].socket, null);

  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, ref: 'REF-1' });
  assert.equal(node.statusCalls[node.statusCalls.length - 1].text, 'sent');
});

test('passes -s <socket> before the call when ubusSocket is configured', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);
  const socketPath = path.join(dir, 'custom-ubus.sock');

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, ubusSocket: socketPath });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].argv, [
    '-t', '30', '-s', socketPath, 'call', 'sms', 'send', JSON.stringify({ to: ['+390000000000'], body: 'hello' })
  ]);
  assert.equal(calls[0].socket, socketPath);
});

test('SEVIO_SMS_UBUS_SOCKET supplies the socket when config has none', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true } }]);
  const socketPath = path.join(dir, 'env-ubus.sock');
  process.env.SEVIO_SMS_UBUS_SOCKET = socketPath;
  t.after(() => { delete process.env.SEVIO_SMS_UBUS_SOCKET; });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].socket, socketPath);
  // Env overrides are diagnosable from the flow editor.
  assert.equal(node.warnings.length, 1);
  assert.match(node.warnings[0], /env-ubus\.sock/);
});

test('SEVIO_SMS_UBUS_BIN supplies the binary when config has none', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'ENV-REF' } }]);
  process.env.SEVIO_SMS_UBUS_BIN = ubusBin;
  t.after(() => { delete process.env.SEVIO_SMS_UBUS_BIN; });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.deepEqual(node.sent[0][0].payload, { ok: true, ref: 'ENV-REF' });
});

test('defaults to the bare `ubus` executable resolved on PATH', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'PATH-REF' } }], 'ubus');
  const originalPath = process.env.PATH;
  process.env.PATH = dir + path.delimiter + originalPath;
  t.after(() => { process.env.PATH = originalPath; });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.deepEqual(node.sent[0][0].payload, { ok: true, ref: 'PATH-REF' });
});

test('success without any ref omits ref from the output payload', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true });
});

test('rpcd reply { ok: false, code } (exit 0) goes to output 2 unchanged', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: false, code: 'not_registered', message: 'sim not registered' } }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, { ok: false, code: 'not_registered', message: 'sim not registered' });
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('ubus-level timeout (exit 249) maps to ubus_timeout with the stderr text', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 249, stderr: 'Command failed: ubus call sms send {} (Timeout)\n' }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, {
    ok: false,
    code: 'ubus_timeout',
    message: 'Command failed: ubus call sms send {} (Timeout)'
  });
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('other ubus-level errors (exit 253) map to ubus_error with the stderr text', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 253, stderr: 'Command failed: ubus call sms send {} (Method not found)\n' }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, {
    ok: false,
    code: 'ubus_error',
    message: 'Command failed: ubus call sms send {} (Method not found)'
  });
});

test('missing ubus binary fails with ubus_unavailable', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const missing = path.join(dir, 'no-such-ubus');

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: missing });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'ubus_unavailable');
  assert.match(failure.payload.message, /no-such-ubus/);
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('multi-recipient alert fans out one ubus CLI call per recipient in order and joins refs', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['+390000000001', '+390000000002', '+390000000003'];
  const recipientsFile = writeRecipients(dir, { 'a-1': recipients });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-2' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-3' } }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, recipients.length);
  assert.deepEqual(
    calls.map((call) => call.data),
    recipients.map((number) => ({ to: [number], body: 'hello' }))
  );
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, ref: 'ref-1,ref-2,ref-3' });
  assert.equal(node.statusCalls[node.statusCalls.length - 1].text, 'sent');
});

test('ubus-level failure on the second recipient aborts the fan-out and reports it', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001', '+390000000002', '+390000000003']
  });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 253, stderr: 'Command failed: ubus call sms send {} (Method not found)\n' },
    { exit: 0, stdout: { ok: true, ref: 'ref-3' } }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  // Give a stray third call time to arrive before asserting the fan-out stopped.
  await delay(50);

  const calls = readCalls(dir);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].data, { to: ['+390000000001'], body: 'hello' });
  assert.deepEqual(calls[1].data, { to: ['+390000000002'], body: 'hello' });
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, {
    ok: false,
    code: 'ubus_error',
    message: 'Command failed: ubus call sms send {} (Method not found)'
  });
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('rpcd failure on the second recipient aborts the fan-out and reports it', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001', '+390000000002', '+390000000003']
  });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 0, stdout: { ok: false, code: 'send_failed', message: 'second recipient failed' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-3' } }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  await delay(50);

  assert.equal(readCalls(dir).length, 2);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, { ok: false, code: 'send_failed', message: 'second recipient failed' });
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('close kills the in-flight ubus child and drops its late result', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001', '+390000000002'] });
  const ubusBin = writeFakeUbus(dir, [{ delayMs: 200, stdout: { ok: true, ref: 'late' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  node.emit('input', { payload: { alertId: 'a-1', body: 'hello' } }, null, () => {});
  await waitFor(() => fs.existsSync(path.join(dir, 'pid')));

  node.emit('close', false, () => {});
  // A result that would land after close must be dropped too.
  await delay(300);

  assert.equal(readCalls(dir).length, 1);
  assert.equal(fs.existsSync(path.join(dir, 'killed')), true, 'the in-flight child received SIGTERM');
  assert.equal(node.sent.length, 0);
  assert.equal(node.errors.length, 0);
  assert.equal(node.statusCalls.some((status) => status.text === 'sent'), false);
  assert.equal(node.statusCalls.some((status) => status.fill === 'red'), false);
});

test('missing recipients file fails with recipients_unavailable', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));

  const node = createNodeFactory()({
    name: 'test',
    recipientsFile: path.join(dir, 'missing.json')
  });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'recipients_unavailable');
});

test('valid JSON but non-object recipients file fails with recipients_unavailable', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, []); // valid JSON, wrong top-level shape

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'recipients_unavailable');
});

test('unknown alertId fails with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'other-alert': ['+390000000000'] });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'no_recipients');
});

test('empty recipient list fails with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': [] });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'no_recipients');
});

test('successful send logs one line with the outcome and ref', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const number = '+390000000000';
  const recipientsFile = writeRecipients(dir, { 'a-1': [number] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);
  const loggerBin = writeFakeLogger(dir);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  await waitFor(() => readLogLines(dir).length === 1);
  assert.deepEqual(readLogLines(dir)[0], [
    '-t', 'sevio-sms', 'alert=a-1 to=1/1 outcome=ok ref=REF-1'
  ]);
  assertNoMsisdn(dir, [number]);
});

test('multi-recipient fan-out logs one line per recipient', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['+390000000001', '+390000000002', '+390000000003'];
  const recipientsFile = writeRecipients(dir, { 'a-1': recipients });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-2' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-3' } }
  ]);
  const loggerBin = writeFakeLogger(dir);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  await waitFor(() => readLogLines(dir).length === recipients.length);
  assert.deepEqual(logLines(dir).sort(), [
    'alert=a-1 to=1/3 outcome=ok ref=ref-1',
    'alert=a-1 to=2/3 outcome=ok ref=ref-2',
    'alert=a-1 to=3/3 outcome=ok ref=ref-3'
  ].sort());
  assertNoMsisdn(dir, recipients);
});

test('fail-fast abort logs the failed attempt and stops there', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['+390000000001', '+390000000002', '+390000000003'];
  const recipientsFile = writeRecipients(dir, { 'a-1': recipients });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 0, stdout: { ok: false, code: 'send_failed', message: 'second recipient failed' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-3' } }
  ]);
  const loggerBin = writeFakeLogger(dir);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  // Give a stray third attempt time to arrive before asserting the fan-out stopped.
  await delay(50);
  await waitFor(() => readLogLines(dir).length === 2);
  assert.deepEqual(logLines(dir).sort(), [
    'alert=a-1 to=1/3 outcome=ok ref=ref-1',
    'alert=a-1 to=2/3 outcome=send_failed message=second recipient failed'
  ].sort());
  assertNoMsisdn(dir, recipients);
});

test('ubus-level timeout logs outcome=ubus_timeout with the stderr message', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const number = '+390000000000';
  const recipientsFile = writeRecipients(dir, { 'a-1': [number] });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 249, stderr: 'Command failed: ubus call sms send {} (Timeout)\n' }
  ]);
  const loggerBin = writeFakeLogger(dir);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  await waitFor(() => readLogLines(dir).length === 1);
  assert.deepEqual(logLines(dir), [
    'alert=a-1 to=1/1 outcome=ubus_timeout message=Command failed: ubus call sms send {} (Timeout)'
  ]);
  assertNoMsisdn(dir, [number]);
});

test('a missing logger binary never breaks the send', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);

  const node = createNodeFactory()({
    name: 'test',
    recipientsFile: recipientsFile,
    ubusBin: ubusBin,
    loggerBin: path.join(dir, 'no-such-logger')
  });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.deepEqual(node.sent[0][0].payload, { ok: true, ref: 'REF-1' });
  assert.equal(node.errors.length, 0);
});

test('SEVIO_SMS_LOGGER_BIN supplies the logger binary when config has none', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'ENV-REF' } }]);
  const previousLoggerBin = process.env.SEVIO_SMS_LOGGER_BIN;
  process.env.SEVIO_SMS_LOGGER_BIN = writeFakeLogger(dir, 'env-logger');
  t.after(() => {
    if (previousLoggerBin === undefined) {
      delete process.env.SEVIO_SMS_LOGGER_BIN;
    } else {
      process.env.SEVIO_SMS_LOGGER_BIN = previousLoggerBin;
    }
  });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  await waitFor(() => readLogLines(dir).length === 1);
  assert.deepEqual(logLines(dir), ['alert=a-1 to=1/1 outcome=ok ref=ENV-REF']);
});

test('loggerTag overrides the default syslog tag', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);
  const loggerBin = writeFakeLogger(dir);

  const node = createNodeFactory()({
    name: 'test',
    recipientsFile: recipientsFile,
    ubusBin: ubusBin,
    loggerBin: loggerBin,
    loggerTag: 'custom-tag'
  });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  await waitFor(() => readLogLines(dir).length === 1);
  assert.equal(readLogLines(dir)[0][1], 'custom-tag');
});
