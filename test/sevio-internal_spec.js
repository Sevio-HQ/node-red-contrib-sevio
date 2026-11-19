const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const Module = require('node:module');

const registerSevioInternal = require('../sevio-internal.js');

function createNodeFactory() {
  const types = {};
  const RED = {
    nodes: {
      createNode(node, config) {
        node.config = config;
        node.statusCalls = [];
        node.handlers = {};
        node.status = (update) => node.statusCalls.push(update);
        node.on = (event, handler) => {
          node.handlers[event] = handler;
        };
      },
      registerType(name, ctor) {
        types[name] = ctor;
      },
    },
  };
  registerSevioInternal(RED);
  return (config) => new types['sevio-internal'](config);
}

const makeNode = createNodeFactory();

/** Starts a server on an ephemeral port; `url` points at `/t`. */
function startServer(handler) {
  return new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => {
      resolve({
        url: `http://127.0.0.1:${server.address().port}/t`,
        close: () => new Promise((done) => server.close(done)),
      });
    });
  });
}

/** Runs one input and resolves with what the node sent or reported. */
function drive(node, msg = {}) {
  return new Promise((resolve) => {
    node.handlers.input(
      msg,
      (sent) => resolve({ sent, err: null }),
      (err) => resolve({ sent: null, err }),
    );
  });
}

const okBody = (readings) => (req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(readings));
};

test('emits the readings and reports them on the status', async () => {
  const server = await startServer(okBody([{ name: 'device_uptime', value: 42, timestamp: 1 }]));
  try {
    const node = makeNode({ url: server.url, groups: '', vrfName: '', timeoutMs: 1000 });
    const msg = { payload: 'previous' };

    const result = await drive(node, msg);

    assert.equal(result.err, null);
    assert.deepEqual(result.sent.payload, [{ name: 'device_uptime', value: 42, timestamp: 1 }]);
    assert.equal(msg.payload.length, 1, 'the input message carries the payload');
    assert.deepEqual(node.statusCalls.at(-1), { fill: 'green', shape: 'dot', text: '1 values' });
  } finally {
    await server.close();
  }
});

test('appends the configured groups to the URL', async () => {
  let seen = null;
  const server = await startServer((req, res) => {
    seen = req.url;
    okBody([])(req, res);
  });
  try {
    const node = makeNode({ url: server.url, groups: 'gps,device', vrfName: '', timeoutMs: 1000 });
    await drive(node);
    assert.equal(seen, '/t?groups=gps%2Cdevice');
  } finally {
    await server.close();
  }
});

test('a non-2xx answer is an error and sends nothing', async () => {
  const server = await startServer((req, res) => {
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end('{"error":"boom"}');
  });
  try {
    const node = makeNode({ url: server.url, groups: '', vrfName: '', timeoutMs: 1000 });
    const result = await drive(node);
    assert.equal(result.sent, null);
    assert.match(result.err.message, /HTTP 500/);
    assert.equal(node.statusCalls.at(-1).fill, 'red');
  } finally {
    await server.close();
  }
});

test('invalid JSON is an error', async () => {
  const server = await startServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('not json');
  });
  try {
    const node = makeNode({ url: server.url, groups: '', vrfName: '', timeoutMs: 1000 });
    const result = await drive(node);
    assert.equal(result.sent, null);
    assert.match(result.err.message, /invalid JSON/);
  } finally {
    await server.close();
  }
});

test('the timeout is a deadline, not an inactivity window', async () => {
  const server = await startServer(() => {
    // accept and never answer
  });
  try {
    const node = makeNode({ url: server.url, groups: '', vrfName: '', timeoutMs: 200 });
    const started = Date.now();
    const result = await drive(node);
    assert.equal(result.sent, null);
    assert.match(result.err.message, /timed out after 200 ms/);
    assert.ok(Date.now() - started < 1000, 'the deadline bounds the request');
  } finally {
    await server.close();
  }
});

test('a truncated response is an error', async () => {
  const server = await startServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': '100' });
    res.write('[{"name":');
    res.destroy();
  });
  try {
    const node = makeNode({ url: server.url, groups: '', vrfName: '', timeoutMs: 1000 });
    const result = await drive(node);
    assert.equal(result.sent, null);
    assert.ok(result.err, 'the request must settle');
  } finally {
    await server.close();
  }
});

test('a malformed URL reports through done, never rejects the handler', async () => {
  const node = makeNode({ url: 'not a url', groups: 'gps', vrfName: '', timeoutMs: 1000 });
  const result = await drive(node);
  assert.equal(result.sent, null);
  assert.match(result.err.message, /Invalid URL/);
});

test('binds the socket to the VRF when sockopt is available', async () => {
  const calls = [];
  const originalLoad = Module._load;
  Module._load = function (request, ...rest) {
    if (request === 'sockopt') {
      return {
        setsockoptString(socket, level, option, value) {
          calls.push({ level, option, value });
        },
      };
    }
    return originalLoad.call(this, request, ...rest);
  };
  const server = await startServer(okBody([]));
  try {
    const factory = createNodeFactory();
    const node = factory({ url: server.url, groups: '', vrfName: 'vrf-lan', timeoutMs: 1000 });
    const result = await drive(node);
    assert.equal(result.err, null);
    assert.deepEqual(calls, [{ level: 1, option: 25, value: 'vrf-lan' }]);
  } finally {
    Module._load = originalLoad;
    await server.close();
  }
});

test('a failed VRF bind settles the input and leaks no socket', async () => {
  const originalLoad = Module._load;
  Module._load = function (request, ...rest) {
    if (request === 'sockopt') {
      return {
        setsockoptString() {
          throw new Error('setsockopt refused');
        },
      };
    }
    return originalLoad.call(this, request, ...rest);
  };
  const server = await startServer(okBody([]));
  try {
    const factory = createNodeFactory();
    const node = factory({ url: server.url, groups: '', vrfName: 'vrf-lan', timeoutMs: 1000 });
    const sockets = () => process.getActiveResourcesInfo().filter((r) => r === 'TCPSocketWrap').length;
    const before = sockets();

    for (let i = 0; i < 10; i += 1) {
      const result = await drive(node);
      assert.match(result.err.message, /setsockopt refused/);
      assert.equal(result.sent, null);
    }
    await new Promise((resolve) => setTimeout(resolve, 100));

    // No growth: a failed bind must not leave descriptors behind. The count may
    // also drop — an earlier test's socket closing is not a leak.
    assert.ok(
      sockets() <= before,
      `a failed bind must not leave descriptors open (before ${before}, after ${sockets()})`,
    );
  } finally {
    Module._load = originalLoad;
    await server.close();
  }
});
