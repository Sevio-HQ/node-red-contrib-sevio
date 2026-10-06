const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
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
        node.sent = [];
        node.handlers = {};
        node.status = (update) => node.statusCalls.push(update);
        node.error = (err, msg) => node.errors.push({ err: err, msg: msg });
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

/** Stub ubus JSON-RPC server; replies are driven by the per-test handler. */
function startStubUbus(socketPath, onRequest) {
  const connections = new Set();
  const server = net.createServer((socket) => {
    connections.add(socket);
    socket.on('close', () => connections.delete(socket));
    socket.on('error', () => {});
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      if (!buffer.includes('\n')) return;
      onRequest(JSON.parse(buffer.trim()), socket);
    });
  });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => {
      resolve({
        close() {
          for (const socket of connections) socket.destroy();
          return new Promise((done) => server.close(done));
        }
      });
    });
  });
}

function sendInput(node, payload) {
  return new Promise((resolve) => {
    node.emit('input', { payload: payload }, null, (err) => resolve(err));
  });
}

const UBUS_OK = { jsonrpc: '2.0', result: [0, { ok: true, ref: '1' }] };

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

test('ubus success reply goes to output 1 with { ok: true, ref }', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const socketPath = path.join(dir, 'ubus.sock');
  const requests = [];
  const stub = await startStubUbus(socketPath, (request, socket) => {
    requests.push(request);
    socket.end(JSON.stringify(UBUS_OK) + '\n');
  });
  t.after(() => stub.close());

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: socketPath });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, ref: '1' });
  assert.equal(requests.length, 1);
  assert.deepEqual(requests[0].params, ['sms', 'send', { to: ['+390000000000'], body: 'hello' }]);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].text, 'sent');
});

test('multi-recipient alert fans out one ubus call per recipient and joins refs', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipients = ['+390000000001', '+390000000002', '+390000000003'];
  const recipientsFile = writeRecipients(dir, { 'a-1': recipients });
  const socketPath = path.join(dir, 'ubus.sock');
  const requests = [];
  const stub = await startStubUbus(socketPath, (request, socket) => {
    requests.push(request);
    socket.end(JSON.stringify({ jsonrpc: '2.0', result: [0, { ok: true, ref: `ref-${requests.length}` }] }) + '\n');
  });
  t.after(() => stub.close());

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: socketPath });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(requests.length, recipients.length);
  assert.deepEqual(
    requests.map((request) => request.params),
    recipients.map((number) => ['sms', 'send', { to: [number], body: 'hello' }])
  );
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true, ref: 'ref-1,ref-2,ref-3' });
  assert.equal(node.statusCalls[node.statusCalls.length - 1].text, 'sent');
});

test('success without any ref omits ref from the output payload', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const socketPath = path.join(dir, 'ubus.sock');
  const stub = await startStubUbus(socketPath, (request, socket) => {
    socket.end(JSON.stringify({ jsonrpc: '2.0', result: [0, { ok: true }] }) + '\n');
  });
  t.after(() => stub.close());

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: socketPath });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  const [success, failure] = node.sent[0];
  assert.equal(failure, null);
  assert.deepEqual(success.payload, { ok: true });
});

test('ubus failure reply goes to output 2 with code and message', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000000'] });
  const socketPath = path.join(dir, 'ubus.sock');
  const stub = await startStubUbus(socketPath, (request, socket) => {
    socket.end(JSON.stringify({ jsonrpc: '2.0', result: [0, { ok: false, code: 'send_failed', message: 'x' }] }) + '\n');
  });
  t.after(() => stub.close());

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: socketPath });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, { ok: false, code: 'send_failed', message: 'x' });
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('failure on the second recipient aborts the fan-out and reports it', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, {
    'a-1': ['+390000000001', '+390000000002', '+390000000003']
  });
  const socketPath = path.join(dir, 'ubus.sock');
  const requests = [];
  const stub = await startStubUbus(socketPath, (request, socket) => {
    requests.push(request);
    const reply = requests.length === 2
      ? { ok: false, code: 'send_failed', message: 'second recipient failed' }
      : { ok: true, ref: 'ref-1' };
    socket.end(JSON.stringify({ jsonrpc: '2.0', result: [0, reply] }) + '\n');
  });
  t.after(() => stub.close());

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: socketPath });
  const err = await sendInput(node, { alertId: 'a-1', body: 'hello' });

  assert.equal(err, undefined);
  // Give a stray third call time to arrive before asserting the fan-out stopped.
  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.equal(requests.length, 2);
  assert.deepEqual(requests[0].params, ['sms', 'send', { to: ['+390000000001'], body: 'hello' }]);
  assert.deepEqual(requests[1].params, ['sms', 'send', { to: ['+390000000002'], body: 'hello' }]);
  assert.equal(node.sent.length, 1);
  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.deepEqual(failure.payload, { ok: false, code: 'send_failed', message: 'second recipient failed' });
  assert.equal(node.errors.length, 1);
  assert.equal(node.statusCalls[node.statusCalls.length - 1].fill, 'red');
});

test('close mid-fan-out drops the late result and the remaining recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': ['+390000000001', '+390000000002'] });
  const socketPath = path.join(dir, 'ubus.sock');
  const requests = [];
  let serverSocket = null;
  let seenRequest;
  const requestSeen = new Promise((resolve) => { seenRequest = resolve; });
  const stub = await startStubUbus(socketPath, (request, socket) => {
    requests.push(request);
    serverSocket = socket;
    seenRequest();
  });
  t.after(() => stub.close());

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: socketPath });
  node.emit('input', { payload: { alertId: 'a-1', body: 'hello' } }, null, () => {});
  await requestSeen;

  node.emit('close', false, () => {});
  // A reply that lands after close must be dropped too.
  if (serverSocket && !serverSocket.destroyed) {
    serverSocket.end(JSON.stringify(UBUS_OK) + '\n');
  }
  await new Promise((resolve) => setTimeout(resolve, 50));

  assert.equal(requests.length, 1);
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
    recipientsFile: path.join(dir, 'missing.json'),
    ubusSocket: path.join(dir, 'ubus.sock')
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

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: path.join(dir, 'ubus.sock') });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'recipients_unavailable');
});

test('unknown alertId fails with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'other-alert': ['+390000000000'] });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: path.join(dir, 'ubus.sock') });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'no_recipients');
});

test('empty recipient list fails with no_recipients', async (t) => {
  const dir = tempDir();
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const recipientsFile = writeRecipients(dir, { 'a-1': [] });

  const node = createNodeFactory()({ name: 'test', recipientsFile: recipientsFile, ubusSocket: path.join(dir, 'ubus.sock') });
  await sendInput(node, { alertId: 'a-1', body: 'hello' });

  const [success, failure] = node.sent[0];
  assert.equal(success, null);
  assert.equal(failure.payload.code, 'no_recipients');
});
