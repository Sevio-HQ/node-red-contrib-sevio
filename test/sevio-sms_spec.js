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

/**
 * Like sendInput, but resolves with a mutable result whose `doneCalls` counts
 * the node's done() invocations: after the test has waited, `doneCalls` must
 * be exactly 1 on every non-closed path.
 */
function sendInputCounting(node, payload) {
  const result = { doneCalls: 0 };
  result.promise = new Promise((resolve) => {
    node.emit('input', { payload: payload }, null, () => {
      result.doneCalls += 1;
      resolve(result);
    });
  });
  return result;
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

test('queue: two node instances share one FIFO slot; alerts serialise in submission order', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001', '+390000000002'],
    'b-1': ['+390000000003']
  });
  // Every call lingers: had the two nodes run concurrently, B's call would be
  // logged while A's fan-out is still waiting on its first recipient.
  const ubusBin = writeFakeUbus(dir, [{ delayMs: 150, stdout: { ok: true, ref: 'ref' } }]);
  const makeNode = createNodeFactory();
  const nodeA = makeNode({ name: 'a', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const nodeB = makeNode({ name: 'b', recipientsFile: recipientsFile, ubusBin: ubusBin });

  const [errA, errB] = await Promise.all([
    sendInput(nodeA, { alertId: 'a-1', body: 'first' }),
    sendInput(nodeB, { alertId: 'b-1', body: 'second' })
  ]);

  assert.equal(errA, undefined);
  assert.equal(errB, undefined);
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), [
    '+390000000001', '+390000000002', '+390000000003'
  ]);
  assert.equal(nodeA.sent[0][0].payload.ok, true);
  assert.equal(nodeB.sent[0][0].payload.ok, true);
});

test('queue: two alerts submitted together on one node never overlap', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001', '+390000000002'],
    'b-1': ['+390000000003']
  });
  const ubusBin = writeFakeUbus(dir, [{ delayMs: 100, stdout: { ok: true, ref: 'ref' } }]);
  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });

  const [errA, errB] = await Promise.all([
    sendInput(node, { alertId: 'a-1', body: 'first' }),
    sendInput(node, { alertId: 'b-1', body: 'second' })
  ]);

  assert.equal(errA, undefined);
  assert.equal(errB, undefined);
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), [
    '+390000000001', '+390000000002', '+390000000003'
  ]);
  assert.equal(node.sent.length, 2);
});

test('queue: recipients are read inside the slot, not at submission time', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    hold: ['+390000000001'],
    'a-1': ['+390000000001']
  });
  const ubusBin = writeFakeUbus(dir, [{ delayMs: 150, stdout: { ok: true, ref: 'ref' } }]);
  const makeNode = createNodeFactory();
  const holder = makeNode({ name: 'holder', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const waiter = makeNode({ name: 'waiter', recipientsFile: recipientsFile, ubusBin: ubusBin });

  const holdPromise = sendInput(holder, { alertId: 'hold', body: 'hold' });
  await waitFor(() => readCalls(dir).length === 1);

  const waitPromise = sendInput(waiter, { alertId: 'a-1', body: 'fresh' });
  // The waiter is queued now: rewrite the file before its turn comes.
  fs.writeFileSync(recipientsFile, JSON.stringify({
    hold: ['+390000000001'],
    'a-1': ['+390000000009']
  }));

  const [errHold, errWait] = await Promise.all([holdPromise, waitPromise]);
  assert.equal(errHold, undefined);
  assert.equal(errWait, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1].data.to, ['+390000000009']);
});

test('queue: waiting past queueWaitMs fails with queue_timeout and makes no ubus call', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001'],
    'b-1': ['+390000000002']
  });
  const ubusBin = writeFakeUbus(dir, [{ delayMs: 400, stdout: { ok: true, ref: 'ref' } }]);
  const makeNode = createNodeFactory();
  const holder = makeNode({ name: 'holder', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const waiter = makeNode({ name: 'waiter', recipientsFile: recipientsFile, ubusBin: ubusBin, queueWaitMs: 100 });

  const holdPromise = sendInput(holder, { alertId: 'a-1', body: 'hold' });
  await waitFor(() => readCalls(dir).length === 1);

  const err = await sendInput(waiter, { alertId: 'b-1', body: 'late' });
  assert.equal(err, undefined);
  const [success, failure] = waiter.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'queue_timeout');
  assert.match(failure.payload.message, /queue/);
  assert.equal(waiter.errors.length, 1);
  assert.equal(waiter.statusCalls[waiter.statusCalls.length - 1].fill, 'red');

  await holdPromise;
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), ['+390000000001']);

  // The queue is not wedged: the waiting node can send after the timeout.
  await sendInput(waiter, { alertId: 'b-1', body: 'after' });
  assert.deepEqual(waiter.sent[1][0].payload, { ok: true, ref: 'ref' });
});

test('queue: close while queued cancels the waiter without sending or done()', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001'],
    'b-1': ['+390000000002']
  });
  const ubusBin = writeFakeUbus(dir, [{ delayMs: 300, stdout: { ok: true, ref: 'ref' } }]);
  const makeNode = createNodeFactory();
  const holder = makeNode({ name: 'holder', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const queued = makeNode({ name: 'queued', recipientsFile: recipientsFile, ubusBin: ubusBin });

  const holdPromise = sendInput(holder, { alertId: 'a-1', body: 'hold' });
  await waitFor(() => readCalls(dir).length === 1);

  let queuedDoneCalls = 0;
  queued.emit('input', { payload: { alertId: 'b-1', body: 'queued' } }, null, () => { queuedDoneCalls += 1; });
  queued.emit('close', false, () => {});

  await holdPromise;
  await delay(50);

  assert.equal(queuedDoneCalls, 0, 'a cancelled waiter is never done()');
  assert.equal(queued.sent.length, 0);
  assert.equal(queued.errors.length, 0);
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), ['+390000000001']);

  // The surviving node still gets the slot afterwards.
  await sendInput(holder, { alertId: 'b-1', body: 'after' });
  assert.equal(holder.sent[1][0].payload.ok, true);
});

test('queue: close while in flight frees the slot for the next sender', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001'],
    'b-1': ['+390000000002']
  });
  const ubusBin = writeFakeUbus(dir, [
    { hang: true },
    { exit: 0, stdout: { ok: true, ref: 'next' } }
  ]);
  const makeNode = createNodeFactory();
  const inFlight = makeNode({ name: 'in-flight', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const next = makeNode({ name: 'next', recipientsFile: recipientsFile, ubusBin: ubusBin });

  let inFlightDoneCalls = 0;
  inFlight.emit('input', { payload: { alertId: 'a-1', body: 'hang' } }, null, () => { inFlightDoneCalls += 1; });
  await waitFor(() => fs.existsSync(path.join(dir, 'pid')));

  inFlight.emit('close', false, () => {});

  // The killed child's callback lands on a closed early-return, which must
  // release the global slot before the next sender's queue wait expires.
  const err = await sendInput(next, { alertId: 'b-1', body: 'next' });
  assert.equal(err, undefined);
  assert.deepEqual(next.sent[0][0].payload, { ok: true, ref: 'next' });
  assert.equal(fs.existsSync(path.join(dir, 'killed')), true);
  assert.equal(inFlightDoneCalls, 0);
  assert.equal(inFlight.sent.length, 0);
  assert.equal(inFlight.errors.length, 0);
});

test('queue: every failure path releases the slot', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 253, stderr: 'Command failed: ubus call sms send {} (Method not found)\n' },
    { exit: 0, stdout: { ok: true, ref: 'recovered' } }
  ]);
  const makeNode = createNodeFactory();
  // A short queue wait turns any leaked slot into a quick, loud failure.
  const noFile = makeNode({
    name: 'no-file',
    recipientsFile: path.join(dir, 'missing.json'),
    ubusBin: ubusBin,
    queueWaitMs: 300
  });
  const flaky = makeNode({ name: 'flaky', recipientsFile: recipientsFile, ubusBin: ubusBin, queueWaitMs: 300 });
  const good = makeNode({ name: 'good', recipientsFile: recipientsFile, ubusBin: ubusBin, queueWaitMs: 300 });

  // 1. recipients_unavailable: fails before any ubus call.
  await sendInput(noFile, { alertId: 'a-1', body: 'one' });
  assert.equal(noFile.sent[0][1].payload.code, 'recipients_unavailable');

  // 2. ubus_error: ubus-level failure on the first attempt.
  await sendInput(flaky, { alertId: 'a-1', body: 'two' });
  assert.equal(flaky.sent[0][1].payload.code, 'ubus_error');

  // 3. A following sender still gets the slot.
  await sendInput(good, { alertId: 'a-1', body: 'three' });
  assert.deepEqual(good.sent[0][0].payload, { ok: true, ref: 'recovered' });
  assert.equal(readCalls(dir).length, 2);
});

test('queue: a busy reply is retried (bounded) and can succeed', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: false, code: 'busy', message: 'modem busy' } },
    { exit: 0, stdout: { ok: true, ref: 'after-busy' } }
  ]);
  const loggerBin = writeFakeLogger(dir);
  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });

  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(readCalls(dir).length, 2);
  assert.deepEqual(node.sent[0][0].payload, { ok: true, ref: 'after-busy' });
  await waitFor(() => readLogLines(dir).length === 2);
  assert.deepEqual(logLines(dir), [
    'alert=a-1 to=1/1 outcome=busy message=modem busy',
    'alert=a-1 to=1/1 outcome=ok ref=after-busy'
  ]);
});

test('queue: busy exhausting the bounded retries surfaces the failure', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: false, code: 'busy', message: 'still busy' } }]);
  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });

  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(readCalls(dir).length, 3); // 1 attempt + 2 bounded retries
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, { ok: false, code: 'busy', message: 'still busy' });
});

test('queue: ubus_timeout and rate_limited are terminal, never retried', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001'],
    'a-2': ['+390000000002']
  });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 249, stderr: 'Command failed: ubus call sms send {} (Timeout)\n' },
    { exit: 0, stdout: { ok: false, code: 'rate_limited', message: 'too many sms' } }
  ]);
  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });

  await sendInput(node, { alertId: 'a-1', body: 'one' });
  await sendInput(node, { alertId: 'a-2', body: 'two' });

  assert.equal(readCalls(dir).length, 2); // exactly one attempt each
  assert.equal(node.sent[0][1].payload.code, 'ubus_timeout');
  assert.equal(node.sent[1][1].payload.code, 'rate_limited');
});

test('queue: done() is called exactly once per input on every path', async (t) => {
  const dir = tempDir();
  const ubusDir = tempDir();
  t.after(() => {
    fs.rmSync(dir, { recursive: true, force: true });
    fs.rmSync(ubusDir, { recursive: true, force: true });
  });
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const okUbus = writeFakeUbus(ubusDir, [{ delayMs: 100, stdout: { ok: true, ref: 'r' } }]);
  const failUbus = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: false, code: 'send_failed', message: 'nope' } }]);
  const makeNode = createNodeFactory();

  // success
  const okNode = makeNode({ name: 'ok', recipientsFile: recipientsFile, ubusBin: okUbus });
  const okResult = await sendInputCounting(okNode, { alertId: 'a-1', body: 'x' }).promise;
  await delay(50);
  assert.equal(okResult.doneCalls, 1, 'success calls done() once');

  // rpcd failure
  const failNode = makeNode({ name: 'fail', recipientsFile: recipientsFile, ubusBin: failUbus });
  const failResult = await sendInputCounting(failNode, { alertId: 'a-1', body: 'x' }).promise;
  await delay(50);
  assert.equal(failResult.doneCalls, 1, 'a failure calls done() once');

  // invalid payload (never queued)
  const invalidResult = await sendInputCounting(okNode, { body: 'x' }).promise;
  await delay(50);
  assert.equal(invalidResult.doneCalls, 1, 'an invalid payload calls done() once');

  // queue timeout
  const holder = makeNode({ name: 'holder', recipientsFile: recipientsFile, ubusBin: okUbus });
  const holdPromise = sendInput(holder, { alertId: 'a-1', body: 'hold' });
  await waitFor(() => readCalls(ubusDir).length === 1);
  const waiter = makeNode({
    name: 'waiter',
    recipientsFile: recipientsFile,
    ubusBin: okUbus,
    queueWaitMs: 50
  });
  const timeoutResult = await sendInputCounting(waiter, { alertId: 'a-1', body: 'late' }).promise;
  await delay(50);
  assert.equal(timeoutResult.doneCalls, 1, 'queue_timeout calls done() once');
  await holdPromise;
});
