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

// One line per invocation: <call>:<pid>:<startedAt>. A lingering child is
// never reaped while the test runs, so the pid is how a test tells whether
// two ubus processes were ever alive at the same time.
const count = fs.readFileSync(path.join(dir, 'calls.log'), 'utf8').trim().split('\\n').filter(Boolean).length;
const recordPid = () => {
  fs.writeFileSync(path.join(dir, 'pid'), String(process.pid));
  fs.appendFileSync(path.join(dir, 'pids.log'), count + ':' + process.pid + ':' + Date.now() + '\\n');
};

process.on('SIGTERM', () => {
  fs.writeFileSync(path.join(dir, 'killed'), '1');
  process.exit(143);
});

const scenario = JSON.parse(fs.readFileSync(path.join(dir, 'scenario.json'), 'utf8'));
const step = scenario.steps[Math.min(count - 1, scenario.steps.length - 1)];

const emit = () => {
  if (step.stdout !== undefined) process.stdout.write(JSON.stringify(step.stdout));
  if (step.stderr !== undefined) process.stderr.write(step.stderr);
  process.exitCode = step.exit || 0;
};

if (step.hang) {
  recordPid();
  setInterval(() => {}, 1000);
} else if (step.delayMs) {
  recordPid();
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

// Every fake-ubus invocation's child: { call, pid, at }, in start order.
function readPids(dir) {
  const file = path.join(dir, 'pids.log');
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map((line) => {
    const [call, pid, at] = line.split(':');
    return { call: Number(call), pid: Number(pid), at: Number(at) };
  });
}

/**
 * True only for a process that is still executing. A child killed with SIGKILL
 * is dead as soon as the kernel takes it, but until Node reaps it the pid still
 * answers `kill(pid, 0)` as a zombie — a zombie runs nothing and holds no
 * modem call, so it is not a second live `ubus`. Off /proc (non-Linux) the
 * pid's mere existence is the best available answer.
 */
function isLive(pid) {
  try {
    process.kill(pid, 0);
  } catch (err) {
    return false;
  }
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    const close = stat.lastIndexOf(')');
    return stat.slice(close + 2, close + 3) !== 'Z';
  } catch (err) {
    return true;
  }
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

// ---------------------------------------------------------------------------
// Recipients artefact: the compatibility contract (§3.3)
//
// The artefact is channel-keyed — `{ <alertId>: { channel, targets } }`. The
// legacy shape `{ <alertId>: [mobiles] }` is accepted as `channel: "sms"` so a
// new node keeps working against an older gateway; the readers ship before the
// artefact changes shape. Another, unknown or missing channel fails closed with
// `channel_mismatch` — never a silent skip and never a fallback to another
// transport. `channel_mismatch` means only "this entry is not my channel", so a
// missing or unusable targets list on the sms channel is `no_recipients`.
// ---------------------------------------------------------------------------

function assertChannelMismatch(node, dir) {
  assert.equal(node.sent.length, 1, 'the input produced exactly one output pair');
  const [success, failure] = node.sent[0];
  assert.equal(success, null, 'a channel mismatch never reports success');
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'channel_mismatch');
  assert.equal(typeof failure.payload.message, 'string');
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
  assert.equal(readCalls(dir).length, 0, 'a channel mismatch must never reach the transport');
}

function assertNoRecipients(node, dir) {
  assert.equal(node.sent.length, 1, 'the input produced exactly one output pair');
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.ok, false);
  assert.equal(failure.payload.code, 'no_recipients');
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
  assert.equal(readCalls(dir).length, 0, 'no recipients must never reach the transport');
}

test('artefact: a legacy bare array is accepted as the sms channel', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['+390000000001', '+390000000002'];
  const recipientsFile = writeRecipients(dir, { 'a-1': recipients });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-2' } }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.deepEqual(readCalls(dir).map((call) => call.data), recipients.map((number) => ({
    to: [number],
    body: 'hello'
  })));
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, ref: 'ref-1,ref-2' });
});

test('artefact: an object with channel "sms" fans out its targets', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['+390000000001', '+390000000002'];
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'sms', targets: recipients }
  });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-2' } }
  ]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.deepEqual(readCalls(dir).map((call) => call.data), recipients.map((number) => ({
    to: [number],
    body: 'hello'
  })));
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, ref: 'ref-1,ref-2' });
});

test('artefact: an email entry fails closed with channel_mismatch and is never sent', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'not-called' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assertChannelMismatch(node, dir);
  // The address that must not be dialled is not echoed into the failure either.
  assert.equal(node.sent[0][1].payload.message.includes('ops@example.com'), false);
});

test('artefact: an unknown channel fails closed with channel_mismatch', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'webhook', targets: ['+390000000001'] }
  });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'not-called' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assertChannelMismatch(node, dir);
});

test('artefact: an entry with no channel fails closed with channel_mismatch', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { targets: ['+390000000001'] }
  });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'not-called' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assertChannelMismatch(node, dir);
});

test('artefact: a non-object entry fails closed with channel_mismatch', async (t) => {
  const cases = [
    { name: 'a string entry', entry: '+390000000001' },
    { name: 'a number entry', entry: 7 }
  ];
  for (const testCase of cases) {
    const dir = tempDir();
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const recipientsFile = writeRecipients(dir, { 'a-1': testCase.entry });
    const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'not-called' } }]);

    const node = createNodeFactory()({ name: testCase.name, recipientsFile: recipientsFile, ubusBin: ubusBin });
    const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

    assert.equal(err, undefined);
    assertChannelMismatch(node, dir);
  }
});

test('artefact: a missing targets key on the sms channel fails with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': { channel: 'sms' } });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'not-called' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assertNoRecipients(node, dir);
  // Not a channel problem: the message names the actual problem.
  assert.equal(node.sent[0][1].payload.message, 'alert a-1 has no usable targets list');
});

test('artefact: targets that are not an array on the sms channel fail with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'sms', targets: '+390000000001' }
  });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'not-called' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assertNoRecipients(node, dir);
  assert.equal(node.sent[0][1].payload.message, 'alert a-1 has no usable targets list');
});

test('artefact: an empty sms target list still fails with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': { channel: 'sms', targets: [] } });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assertNoRecipients(node, dir);
  assert.equal(node.sent[0][1].payload.message, 'alert a-1 has no usable targets list');
});

test('artefact: a missing entry keeps no_recipients even when other channels are present', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-2': { channel: 'email', targets: ['ops@example.com'] }
  });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'no_recipients');
});

test('artefact: an email entry for another alert does not disturb an sms send', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'email-alert': { channel: 'email', targets: ['ops@example.com'] },
    'a-1': { channel: 'sms', targets: ['+390000000001'] }
  });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].data, { to: ['+390000000001'], body: 'hello' });
  assert.deepEqual(node.sent[0][0].payload, { ok: true, ref: 'REF-1' });
});

test('artefact: a channel mismatch calls done() exactly once', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': { channel: 'email', targets: ['ops@example.com'] }
  });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile });
  const result = await sendInputCounting(node, { alertId: 'a-1', body: 'hello' }).promise;
  await delay(50);

  assert.equal(result.doneCalls, 1);
  assert.equal(node.sent[0][1].payload.code, 'channel_mismatch');
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

test('charset: a mixed body truncates at the first uncarriable character, with dots appended, and the truncation is logged once', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['+390000000001', '+390000000002'];
  const recipientsFile = writeRecipients(dir, { 'a-1': recipients });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },
    { exit: 0, stdout: { ok: true, ref: 'ref-2' } }
  ]);
  const loggerBin = writeFakeLogger(dir);
  const body = 'Livello € carburante basso — trattore 12 è fermo';
  const expected = 'Livello ...'; // everything from the € on is gone; not '... basso...'

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: body });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 2);
  for (const call of calls) {
    // Exact bytes: the tail from the first uncarriable character on is gone,
    // replaced by three ASCII dots. Nothing after that character survives.
    assert.equal(
      Buffer.compare(Buffer.from(call.data.body, 'utf8'), Buffer.from(expected, 'utf8')),
      0,
      `sent body bytes differ: ${JSON.stringify(call.data.body)}`
    );
  }
  // One truncation warning per alert (not per recipient) plus two outcome
  // lines; the warning names the trigger and the 40 dropped characters
  // (48 total code points minus the 8 kept).
  await waitFor(() => readLogLines(dir).length === 3);
  await delay(50); // give a stray second warning time to arrive
  assert.deepEqual(logLines(dir).sort(), [
    'alert=a-1 outcome=body_truncated trigger="€" dropped=40',
    'alert=a-1 to=1/2 outcome=ok ref=ref-1',
    'alert=a-1 to=2/2 outcome=ok ref=ref-2'
  ].sort());
  assertNoMsisdn(dir, recipients);
});

test('charset: a printable-ASCII body — GSM extension characters included — is sent completely unmodified, with no dots and no warning', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);
  const loggerBin = writeFakeLogger(dir);
  // Every printable ASCII code point, prefixed with the GSM extension
  // characters that must not be mistaken for uncarriable ones.
  const allPrintable = Array.from(
    { length: 0x7e - 0x20 + 1 },
    (_, i) => String.fromCharCode(0x20 + i)
  ).join('');
  const body = `[ ] { } \\ ~ | ^ ${allPrintable}`;

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: body });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].data.body, body);
  assert.equal(
    Buffer.compare(Buffer.from(calls[0].data.body, 'utf8'), Buffer.from(body, 'utf8')),
    0,
    'a printable-ASCII body must reach ubus byte-identical'
  );
  assert.equal(calls[0].data.body.endsWith('...'), false, 'a clean body must not gain trailing dots');
  // Exactly one log line — the outcome. No truncation warning when nothing was cut.
  await waitFor(() => readLogLines(dir).length === 1);
  await delay(50); // give a stray truncation warning time to arrive
  assert.deepEqual(logLines(dir), ['alert=a-1 to=1/1 outcome=ok ref=REF-1']);
});

test('charset: a control character truncates the body too, named with escapes in the warning', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);
  const loggerBin = writeFakeLogger(dir);
  const body = 'Low\nFuel\ttractor\u000712';
  const expected = 'Low...'; // the \n is the first uncarriable character

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: body });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].data.body, expected);
  await waitFor(() => readLogLines(dir).length === 2);
  assert.deepEqual(logLines(dir).sort(), [
    'alert=a-1 outcome=body_truncated trigger="\\n" dropped=16',
    'alert=a-1 to=1/1 outcome=ok ref=REF-1'
  ].sort());
});

test('charset: a bad character late in a near-160-septet body truncates the prefix until the result fits the budget', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);
  const loggerBin = writeFakeLogger(dir);
  // 158 printable septets, then the bad character: 158 + 3 dots would be 161,
  // one septet over the 160 budget, so the kept prefix loses one character.
  const body = 'a'.repeat(158) + '€';
  const expected = 'a'.repeat(157) + '...';

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: body });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].data.body, expected);
  assert.equal(expected.length, 160, 'the result sits exactly on the 160-septet budget');
  assert.equal(
    Buffer.compare(Buffer.from(calls[0].data.body, 'utf8'), Buffer.from(expected, 'utf8')),
    0,
    `sent body bytes differ: ${JSON.stringify(calls[0].data.body)}`
  );
  await waitFor(() => readLogLines(dir).length === 2);
  assert.deepEqual(logLines(dir).sort(), [
    'alert=a-1 outcome=body_truncated trigger="€" dropped=2',
    'alert=a-1 to=1/1 outcome=ok ref=REF-1'
  ].sort());
});

test('charset: GSM extension characters in the kept prefix count two septets each towards the budget', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'REF-1' } }]);
  const loggerBin = writeFakeLogger(dir);
  // The prefix is 158 septets (156 single-septet b's plus the '[' at two),
  // so 158 + 3 dots is over budget: dropping the 2-septet '[' is enough, and
  // miscounting '[' as one septet would keep it and end at 161.
  const body = 'b'.repeat(156) + '[' + '€';
  const expected = 'b'.repeat(156) + '...';

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin, loggerBin: loggerBin });
  const err = await sendInput(node, { alertId: 'a-1', body: body });

  assert.equal(err, undefined);
  const calls = readCalls(dir);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].data.body, expected);
  // 156 b's (156 septets) + 3 dots: 159 septets, within budget.
  assert.equal(expected.length, 159);
  await waitFor(() => readLogLines(dir).length === 2);
  assert.deepEqual(logLines(dir).sort(), [
    'alert=a-1 outcome=body_truncated trigger="€" dropped=2',
    'alert=a-1 to=1/1 outcome=ok ref=REF-1'
  ].sort());
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

test('queue: rpcd timeout, send_failed and not_registered are terminal with exactly one call', async (t) => {
  for (const code of ['timeout', 'send_failed', 'not_registered']) {
    const dir = tempDir();
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
    const ubusBin = writeFakeUbus(dir, [
      { exit: 0, stdout: { ok: false, code: code, message: `rpcd ${code}` } }
    ]);
    const node = createNodeFactory()({ name: code, recipientsFile: recipientsFile, ubusBin: ubusBin });

    const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

    assert.equal(err, undefined);
    // The input resolves on the first failure; any retry would only surface
    // after BUSY_RETRY_DELAY_MS, so this assertion pins "no retry" for the
    // codes where a retry could duplicate the SMS.
    assert.equal(readCalls(dir).length, 1, `${code} must be attempted exactly once`);
    const [success, failure] = node.sent[0];
    assert.equal(success, null);
    assert.deepEqual(failure.payload, { ok: false, code: code, message: `rpcd ${code}` });
    assert.equal(node.errors.length, 1);
    assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
  }
});

test('queue: holder plus two waiters are served in FIFO order', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    hold: ['+390000000001'],
    w1: ['+390000000002'],
    w2: ['+390000000003']
  });
  const ubusBin = writeFakeUbus(dir, [{ delayMs: 120, stdout: { ok: true, ref: 'ref' } }]);
  const makeNode = createNodeFactory();
  const holder = makeNode({ name: 'holder', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const first = makeNode({ name: 'first', recipientsFile: recipientsFile, ubusBin: ubusBin });
  const second = makeNode({ name: 'second', recipientsFile: recipientsFile, ubusBin: ubusBin });

  const holdPromise = sendInput(holder, { alertId: 'hold', body: 'hold' });
  await waitFor(() => readCalls(dir).length === 1);

  // Both waiters queue while the holder still owns the slot; the second
  // submission must not jump the first (a pop() would serve it first).
  const firstPromise = sendInput(first, { alertId: 'w1', body: 'first' });
  const secondPromise = sendInput(second, { alertId: 'w2', body: 'second' });

  const [errHold, errFirst, errSecond] = await Promise.all([holdPromise, firstPromise, secondPromise]);
  assert.equal(errHold, undefined);
  assert.equal(errFirst, undefined);
  assert.equal(errSecond, undefined);
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), [
    '+390000000001', '+390000000002', '+390000000003'
  ]);
  assert.equal(holder.sent[0][0].payload.ok, true);
  assert.equal(first.sent[0][0].payload.ok, true);
  assert.equal(second.sent[0][0].payload.ok, true);
});

test('queue: hung ubus child is SIGKILLed after killTimeoutMs', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  const ubusBin = writeFakeUbus(dir, [{ hang: true }]);

  const node = createNodeFactory()({
    name: 'test',
    recipientsFile: recipientsFile,
    ubusBin: ubusBin,
    killTimeoutMs: 200
  });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'ubus_timeout');
  assert.match(failure.payload.message, /killed after 200ms/);

  // SIGKILL cannot be caught: the fake ubus never gets to write its SIGTERM
  // marker (unlike the child.kill() on the close path) and the pid is gone.
  const pid = Number(fs.readFileSync(path.join(dir, 'pid'), 'utf8'));
  await waitFor(() => {
    try {
      process.kill(pid, 0);
      return false;
    } catch (killErr) {
      return killErr.code === 'ESRCH';
    }
  });
  assert.equal(fs.existsSync(path.join(dir, 'killed')), false, 'a SIGKILLed child cannot record SIGTERM');
});

test('queue: the slot watchdog aborts the wedged run, which stops sending and fails once', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    wedged: ['+390000000001', '+390000000002'],
    next: ['+390000000003']
  });
  // The wedged run's first call outlives its slotMaxMs; the second step serves
  // the alert that takes the slot.
  const ubusBin = writeFakeUbus(dir, [
    { delayMs: 300, stdout: { ok: true, ref: 'wedged-1' } },
    { exit: 0, stdout: { ok: true, ref: 'next' } }
  ]);
  const makeNode = createNodeFactory();
  const wedged = makeNode({
    name: 'wedged',
    recipientsFile: recipientsFile,
    ubusBin: ubusBin,
    slotMaxMs: 150
  });
  const next = makeNode({ name: 'next', recipientsFile: recipientsFile, ubusBin: ubusBin });

  wedged.emit('input', { payload: { alertId: 'wedged', body: 'wedged' } }, null, () => {});
  await waitFor(() => readCalls(dir).length === 1);

  const err = await sendInput(next, { alertId: 'next', body: 'next' });
  assert.equal(err, undefined);
  assert.deepEqual(next.sent[0][0].payload, { ok: true, ref: 'next' });

  // The watchdog has already aborted the wedged run before the slot changed
  // hands: its in-flight call was killed, so the slow reply never arrives and
  // the second recipient is never attempted.
  await delay(250);
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), [
    '+390000000001', '+390000000003'
  ]);
  // The abort is the abandoned run's terminal output: exactly one failure,
  // never silence and never a success for a send the modem did not confirm.
  assert.equal(wedged.sent.length, 1, 'the abandoned run emits exactly one failure');
  const [wedgedSuccess, wedgedFailure] = wedged.sent[0];
  assert.equal(wedgedSuccess, null);
  assert.equal(wedgedFailure.payload.ok, false);
  assert.equal(wedgedFailure.payload.code, 'outcome_unknown');
});

// ---------------------------------------------------------------------------
// The SLOT watchdog — distinct from the per-child kill backstop just above.
// The backstop ends one ubus call; the slot watchdog ends the whole run so the
// slot can change hands. It must abort the run (SIGKILL the child, cancel the
// retry timers, emit one failure, stop the fan-out) BEFORE the slot transfers:
// releasing the slot while the abandoned child is still alive puts two ubus
// calls on the modem at once — and can deliver the same SMS twice, which is
// exactly what the slot exists to prevent.
// ---------------------------------------------------------------------------

test('queue: the slot watchdog SIGKILLs the live ubus child before the slot changes hands, and emits one failure', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001', '+390000000002', '+390000000003'],
    'b-1': ['+390000000004']
  });
  // Every child lingers and never answers: the abandoned run still has a live
  // ubus process when the slot bound expires.
  const ubusBin = writeFakeUbus(dir, [{ hang: true }]);
  const loggerBin = writeFakeLogger(dir);
  const makeNode = createNodeFactory();

  // Observe the node's own timers: the abandoned child's kill backstop must be
  // cleared once its callback lands, or the aborted run would keep a timer
  // alive behind the alert that now holds the slot.
  const timers = new Map();
  const realSetTimeout = global.setTimeout;
  const realClearTimeout = global.clearTimeout;
  global.setTimeout = function (fn, ms) {
    const handle = realSetTimeout.apply(this, arguments);
    timers.set(handle, { ms: ms, cleared: false });
    return handle;
  };
  global.clearTimeout = function (handle) {
    if (timers.has(handle)) timers.get(handle).cleared = true;
    return realClearTimeout.apply(this, arguments);
  };
  t.after(() => {
    global.setTimeout = realSetTimeout;
    global.clearTimeout = realClearTimeout;
  });

  // Accelerated slot bound and a labelled per-child backstop: whatever kills
  // the child is the slot watchdog, not the 35 s default backstop.
  const holder = makeNode({
    name: 'holder',
    recipientsFile: recipientsFile,
    ubusBin: ubusBin,
    loggerBin: loggerBin,
    slotMaxMs: 120,
    killTimeoutMs: 60000
  });
  const behind = makeNode({
    name: 'behind',
    recipientsFile: recipientsFile,
    ubusBin: ubusBin,
    slotMaxMs: 120
  });
  // Unconditional cleanup: an assertion failure must still kill the hung
  // children, or the runner would wait for them forever.
  const closeAll = () => {
    behind.emit('close', false, () => {});
    holder.emit('close', false, () => {});
  };
  t.after(closeAll);

  // Observation: how many children are live at each sample. The slot exists so
  // that this number is never above one.
  const samples = [];
  const poller = setInterval(() => {
    samples.push(readPids(dir).filter((record) => isLive(record.pid)).length);
  }, 2);
  t.after(() => clearInterval(poller));

  // Observation: the moment the node kills a child, so the kill can be placed
  // in time relative to the next child's start.
  const kills = [];
  const childProcess = require('node:child_process');
  const realKill = childProcess.ChildProcess.prototype.kill;
  childProcess.ChildProcess.prototype.kill = function (signal) {
    kills.push({ pid: this.pid, signal: signal, at: Date.now() });
    return realKill.apply(this, arguments);
  };
  t.after(() => { childProcess.ChildProcess.prototype.kill = realKill; });

  holder.emit('input', { payload: { alertId: 'a-1', body: 'x' } }, null, () => {});
  await waitFor(() => readPids(dir).length === 1);
  const holderChild = readPids(dir)[0];

  // The second node queues behind the holder: its turn IS the slot transfer.
  behind.emit('input', { payload: { alertId: 'b-1', body: 'x' } }, null, () => {});

  // The abort is what terminates the abandoned input: without it the run would
  // hang on its child and this never happens.
  await waitFor(() => holder.sent.length === 1, 3000);
  // The queued node's turn: its child records the pid a moment after it logs
  // the call, so wait for the pid record — that is the slot transfer.
  await waitFor(() => readPids(dir).some((record) => record.call === 2), 3000);

  // 1. Exactly one terminal output, on output 2, and the code is
  //    outcome_unknown: the slot expired around a send the modem may already
  //    have accepted, so it is never a retry.
  assert.equal(holder.sent.length, 1, 'exactly one output for the aborted run');
  const [success, failed] = holder.sent[0];
  assert.equal(success, null);
  assert.equal(failed.payload.ok, false);
  assert.equal(failed.payload.code, 'outcome_unknown');
  assert.equal(failed.payload.accepted, 0);
  assert.equal(failed.payload.total, 3);
  assert.match(failed.payload.message, /slot expired/);
  assert.equal(holder.errors.length, 1);

  // 2. The live child is dead, and it was SIGKILLed (the fake records a
  //    SIGTERM marker, so an absent marker proves the kill was not SIGTERM).
  const holderKill = kills.find((kill) => kill.pid === holderChild.pid);
  assert.ok(holderKill, 'the slot watchdog killed the abandoned run\'s child');
  assert.equal(holderKill.signal, 'SIGKILL');
  await waitFor(() => !isLive(holderChild.pid), 1000);
  assert.equal(fs.existsSync(path.join(dir, 'killed')), false, 'the slot kill is SIGKILL, not SIGTERM');

  // 3. The run's own timers are cleared: the abandoned child's 60 s kill
  //    backstop is gone as soon as its callback lands, so nothing of this run
  //    is left ticking behind the alert that now holds the slot.
  const backstops = [...timers.values()].filter((timer) => timer.ms === 60000);
  assert.equal(backstops.length, 1, 'the abandoned child had exactly one kill backstop');
  await waitFor(() => backstops[0].cleared === true, 1000);
  assert.equal(backstops.every((timer) => timer.cleared), true, 'the aborted run left no timer pending');

  // 4. The fan-out stopped: recipients 2 and 3 of the abandoned run are never
  //    attempted. Call 2 belongs to the alert that now holds the slot.
  const calls = readCalls(dir);
  assert.deepEqual(calls.map((call) => call.data.to[0]), ['+390000000001', '+390000000004']);

  // 5. The kill is issued BEFORE the slot changes hands: the next child starts
  //    only after the abandoned one was killed.
  const behindChild = readPids(dir).find((record) => record.call === 2);
  assert.ok(behindChild, 'the queued node takes the slot');
  assert.ok(
    holderKill.at <= behindChild.at,
    `the abandoned child was killed at ${holderKill.at}, the next child started at ${behindChild.at}`
  );

  // 6. The abort is syslog-visible once, by position: an MSISDN is never
  //    logged, and the run is never silent about how it ended.
  await waitFor(() => logLines(dir).length === 1, 2000);
  assert.match(logLines(dir)[0], /^alert=a-1 to=1\/3 outcome=outcome_unknown$/);
  assert.equal(logLines(dir)[0].includes('+3900'), false, 'position-only logging');

  // 7. No second failure ever appears, even after the killed child's own close
  //    callback lands.
  await delay(120);
  assert.equal(holder.sent.length, 1, 'the kill callback must not emit a second failure');
  assert.equal(holder.errors.length, 1);

  // 8. Never more than one live child at any moment.
  assert.equal(
    Math.max(0, ...samples),
    1,
    `two children were alive at once (${samples.filter((n) => n > 1).length} of ${samples.length} samples)`
  );
});

test('queue: the slot watchdog clears a pending busy-retry timer: the abandoned run never retries', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001'] });
  // A `busy` reply arms the retry timer; the slot bound expires while the run
  // is still waiting to retry, so the timer is pending at the abort.
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: false, code: 'busy', message: 'modem busy' } },
    { exit: 0, stdout: { ok: true, ref: 'retry' } }
  ]);

  // Observe every timer the node creates: the retry timer is the one created
  // with BUSY_RETRY_DELAY_MS (1000 ms), and the abort must have cleared it.
  const timers = new Map();
  const realSetTimeout = global.setTimeout;
  const realClearTimeout = global.clearTimeout;
  global.setTimeout = function (fn, ms) {
    const handle = realSetTimeout.apply(this, arguments);
    timers.set(handle, { ms: ms, cleared: false });
    return handle;
  };
  global.clearTimeout = function (handle) {
    if (timers.has(handle)) timers.get(handle).cleared = true;
    return realClearTimeout.apply(this, arguments);
  };
  t.after(() => {
    global.setTimeout = realSetTimeout;
    global.clearTimeout = realClearTimeout;
  });

  const node = createNodeFactory()({
    name: 'busy-holder',
    recipientsFile: recipientsFile,
    ubusBin: ubusBin,
    slotMaxMs: 150 // expires while the retry waits
  });
  // The abort is what ends this input: a watchdog that only releases the slot
  // leaves the run abandoned with no terminal output at all.
  await sendInput(node, { alertId: 'a-1', body: 'busy' });

  // One busy attempt, one failure — no retry behind the next alert.
  await delay(1100);
  assert.equal(readCalls(dir).length, 1, 'the abandoned run never retries');
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'outcome_unknown');
  assert.equal(failure.payload.accepted, 0);
  assert.equal(failure.payload.total, 1);

  const retryTimers = [...timers.values()].filter((timer) => timer.ms === 1000);
  assert.equal(retryTimers.length, 1, 'the busy reply armed exactly one retry timer');
  assert.equal(retryTimers[0].cleared, true, 'the slot abort cleared the pending retry timer');
});

test('queue: close during the busy-retry delay drops the slot and cancels the retry', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001'],
    'b-1': ['+390000000002']
  });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: false, code: 'busy', message: 'modem busy' } },
    { exit: 0, stdout: { ok: true, ref: 'next' } }
  ]);
  const makeNode = createNodeFactory();
  // Watchdog long enough not to fire during the test, short enough that a
  // leaked slot cannot keep a failing run alive for the 240 s default.
  const busy = makeNode({ name: 'busy', recipientsFile: recipientsFile, ubusBin: ubusBin, slotMaxMs: 5000 });
  const next = makeNode({ name: 'next', recipientsFile: recipientsFile, ubusBin: ubusBin, queueWaitMs: 300 });

  let busyDoneCalls = 0;
  busy.emit('input', { payload: { alertId: 'a-1', body: 'busy' } }, null, () => { busyDoneCalls += 1; });
  await waitFor(() => readCalls(dir).length === 1);
  await delay(100); // let the busy reply land and the retry timer start

  busy.emit('close', false, () => {});

  // The next sender must get the slot well before the 1 s retry delay; if the
  // close only stopped the retry without dropping, this would queue_timeout.
  const err = await sendInput(next, { alertId: 'b-1', body: 'next' });
  assert.equal(err, undefined);
  assert.deepEqual(next.sent[0][0].payload, { ok: true, ref: 'next' });

  // The cancelled retry never reaches ubus.
  await delay(1100);
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), [
    '+390000000001', '+390000000002'
  ]);
  assert.equal(busyDoneCalls, 0, 'a closed node never done()s the dropped input');
  assert.equal(busy.sent.length, 0);
  assert.equal(busy.errors.length, 0);
});

test('queue: close during the readRecipients window drops the slot', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001'],
    'b-1': ['+390000000002']
  });
  const ubusBin = writeFakeUbus(dir, [{ exit: 0, stdout: { ok: true, ref: 'next' } }]);
  const makeNode = createNodeFactory();
  // See the busy-retry close test: keep a leaked slot's watchdog short.
  const closing = makeNode({ name: 'closing', recipientsFile: recipientsFile, ubusBin: ubusBin, slotMaxMs: 5000 });
  const next = makeNode({ name: 'next', recipientsFile: recipientsFile, ubusBin: ubusBin, queueWaitMs: 1000 });

  // Hold the closing node's recipients read open so close lands in that window.
  const originalReadFile = fs.readFile;
  let gated = true;
  let readStarted;
  const started = new Promise((resolve) => { readStarted = resolve; });
  let releaseRead;
  const gate = new Promise((resolve) => { releaseRead = resolve; });
  fs.readFile = function (file, encoding, callback) {
    if (gated && file === recipientsFile) {
      gated = false;
      readStarted();
      gate.then(() => originalReadFile.call(fs, file, encoding, callback));
      return;
    }
    originalReadFile.call(fs, file, encoding, callback);
  };
  t.after(() => { fs.readFile = originalReadFile; });

  let closingDoneCalls = 0;
  closing.emit('input', { payload: { alertId: 'a-1', body: 'closing' } }, null, () => { closingDoneCalls += 1; });
  await started;
  closing.emit('close', false, () => {});

  const nextPromise = sendInput(next, { alertId: 'b-1', body: 'next' });
  releaseRead();

  const err = await nextPromise;
  assert.equal(err, undefined);
  assert.deepEqual(next.sent[0][0].payload, { ok: true, ref: 'next' });
  assert.deepEqual(readCalls(dir).map((call) => call.data.to[0]), ['+390000000002']);
  assert.equal(closingDoneCalls, 0, 'a closed node never done()s the dropped input');
  assert.equal(closing.sent.length, 0);
  assert.equal(closing.errors.length, 0);
});

test('queue: the busy retry budget is fresh for each recipient', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001', '+390000000002'] });
  const ubusBin = writeFakeUbus(dir, [
    { exit: 0, stdout: { ok: false, code: 'busy', message: 'busy 1' } },  // recipient 1, attempt 1
    { exit: 0, stdout: { ok: true, ref: 'ref-1' } },                     // recipient 1, retry 1
    { exit: 0, stdout: { ok: false, code: 'busy', message: 'busy 2' } },  // recipient 2, attempt 1
    { exit: 0, stdout: { ok: false, code: 'busy', message: 'busy 3' } },  // recipient 2, retry 1
    { exit: 0, stdout: { ok: true, ref: 'ref-2' } }                      // recipient 2, retry 2
  ]);
  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusBin: ubusBin });

  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(readCalls(dir).length, 5);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, ref: 'ref-1,ref-2' });
});

test('queue: an invalid queueWaitMs falls back to the default wait', async (t) => {
  const cases = [
    { name: 'negative config', config: { queueWaitMs: -5 } },
    { name: 'NaN config', config: { queueWaitMs: 'nope' } },
    { name: 'config above the setTimeout max', config: { queueWaitMs: 2147483648 } },
    { name: 'env negative', config: { env: '-5' } }
  ];
  for (const testCase of cases) {
    const dir = tempDir();
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const recipientsFile = writeRecipients(dir, {
      hold: ['+390000000001'],
      'a-1': ['+390000000002']
    });
    const ubusBin = writeFakeUbus(dir, [{ delayMs: 250, stdout: { ok: true, ref: 'ref' } }]);
    const makeNode = createNodeFactory();
    const holder = makeNode({ name: 'holder', recipientsFile: recipientsFile, ubusBin: ubusBin });
    const waiterConfig = { name: 'waiter', recipientsFile: recipientsFile, ubusBin: ubusBin };
    if (testCase.config.queueWaitMs !== undefined) {
      waiterConfig.queueWaitMs = testCase.config.queueWaitMs;
    }
    if (testCase.config.env !== undefined) {
      process.env.SEVIO_SMS_QUEUE_WAIT_MS = testCase.config.env;
      t.after(() => { delete process.env.SEVIO_SMS_QUEUE_WAIT_MS; });
    }
    const waiter = makeNode(waiterConfig);

    const holdPromise = sendInput(holder, { alertId: 'hold', body: 'hold' });
    await waitFor(() => readCalls(dir).length === 1);
    const waitPromise = sendInput(waiter, { alertId: 'a-1', body: 'wait' });

    const [errHold, errWait] = await Promise.all([holdPromise, waitPromise]);
    assert.equal(errHold, undefined);
    assert.equal(errWait, undefined);
    assert.equal(
      waiter.sent[0][1],
      null,
      `${testCase.name}: queueWaitMs must fall back to the default, not fire in ~1 ms`
    );
    assert.deepEqual(waiter.sent[0][0].payload, { ok: true, ref: 'ref' });
  }
});
