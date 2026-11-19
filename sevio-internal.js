module.exports = (RED) => {
  const http = require('http');
  const net = require('net');

  const DEFAULT_URL = 'http://127.0.0.1:3001/internal/telemetry';
  const DEFAULT_VRF = 'vrf-lan';
  const DEFAULT_TIMEOUT_MS = 5000;

  // SOL_SOCKET / SO_BINDTODEVICE as defined by Linux (asm-generic/socket.h).
  const SOL_SOCKET = 1;
  const SO_BINDTODEVICE = 25;

  /**
   * `sockopt` ships with the router firmware (`/usr/lib/node/sockopt`, symlinked
   * into `/usr/lib/node_modules`), not with this package: on a dev machine or in
   * the docker harness the require fails and the node falls back to a plain
   * socket, which is exactly what those environments need.
   */
  let sockopt;
  let sockoptLoaded = false;
  const loadSockopt = () => {
    if (sockoptLoaded) return sockopt;
    sockoptLoaded = true;
    try {
      sockopt = require('sockopt');
    } catch (err) {
      sockopt = null;
    }
    return sockopt;
  };

  /**
   * A socket bound to a VRF device, the same trick `node-red-contrib-modbus`
   * uses for its Modbus client: `SO_BINDTODEVICE` must be set once the handle
   * exists and before `connect()`, so the option is applied from the `_handle`
   * setter. `handle.bind('0.0.0.0', 0)` gives the handle a local address first —
   * `setsockopt` needs a bound socket for this option.
   *
   * The setter can throw (setsockopt failure): the caller destroys the socket
   * and rethrows, so a failed bind never leaves an open descriptor behind.
   */
  const vrfSocket = (vrfName) => {
    const { setsockoptString } = loadSockopt();
    const socket = new net.Socket();
    const handleSymbol = Object.getOwnPropertySymbols(socket).find((symbol) => symbol.description === 'kHandle');
    let handle;

    Object.defineProperty(socket, '_handle', {
      get: () => handle,
      set: (value) => {
        handle = value;
        // Node's stream internals read the symbol, not the property.
        if (handleSymbol) socket[handleSymbol] = value;
        if (value && typeof value.bind === 'function') {
          const bound = value.bind('0.0.0.0', 0);
          if (typeof bound === 'number' && bound !== 0) {
            throw new Error(`socket bind failed (${bound})`);
          }
          setsockoptString(socket, SOL_SOCKET, SO_BINDTODEVICE, vrfName);
        }
      },
      configurable: true,
    });

    return socket;
  };

  /**
   * GET `url` and resolve the parsed JSON body; `vrfName` empty means plain.
   *
   * Every failure path settles the promise exactly once and leaves no socket
   * behind: a synchronous failure while creating the VRF socket destroys it,
   * a truncated response (headers, then a close without `end`) is an error,
   * and the timeout is an overall deadline rather than an inactivity timer —
   * a stalled connect or a trickling body cannot outlive it.
   */
  const fetchTelemetry = (url, vrfName, timeoutMs) =>
    new Promise((resolve, reject) => {
      let settled = false;
      let timer = null;
      const finish = (err, value) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        if (err) reject(err);
        else resolve(value);
      };

      let target;
      try {
        target = new URL(url);
      } catch (err) {
        finish(new Error(`invalid url: ${err.message}`));
        return;
      }
      if (target.protocol !== 'http:') {
        finish(new Error(`unsupported protocol ${target.protocol}: use http`));
        return;
      }

      const port = Number(target.port || 80);
      const options = {
        hostname: target.hostname,
        port,
        path: `${target.pathname}${target.search}`,
        method: 'GET',
        // One fresh socket per poll: the global agent's keep-alive would hold
        // descriptors between polls and could hand back a socket the API has
        // already closed (a restart). The connection is closed from the request
        // side because `agent: false` would drop `createConnection` — the VRF
        // socket — and the node would silently bypass the bind.
        headers: { accept: 'application/json', connection: 'close' },
      };

      if (vrfName && loadSockopt()) {
        options.createConnection = () => {
          const socket = vrfSocket(vrfName);
          try {
            socket.connect(port, target.hostname);
          } catch (err) {
            socket.destroy();
            throw err;
          }
          return socket;
        };
      }

      let req;
      try {
        req = http.request(options, (res) => {
          const chunks = [];
          res.on('data', (chunk) => chunks.push(chunk));
          res.on('error', finish);
          res.on('aborted', () => finish(new Error('response aborted')));
          res.on('end', () => {
            const body = Buffer.concat(chunks).toString('utf8');
            if (res.statusCode < 200 || res.statusCode >= 300) {
              finish(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`));
              return;
            }
            try {
              finish(null, JSON.parse(body));
            } catch (err) {
              finish(new Error(`invalid JSON: ${err.message}`));
            }
          });
        });
      } catch (err) {
        finish(err);
        return;
      }

      req.on('error', finish);
      timer = setTimeout(() => {
        const err = new Error(`timed out after ${timeoutMs} ms`);
        req.destroy(err);
        finish(err);
      }, timeoutMs);
      req.end();
    });

  function SevioInternalNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.url = config.url || DEFAULT_URL;
    node.groups = config.groups || '';
    node.vrfName = config.vrfName === undefined ? DEFAULT_VRF : config.vrfName;
    const configuredTimeout = Number(config.timeoutMs);
    node.timeoutMs = Number.isFinite(configuredTimeout) && configuredTimeout > 0 ? configuredTimeout : DEFAULT_TIMEOUT_MS;

    node.on('input', async (msg, send, done) => {
      try {
        // Inside the try: a malformed URL must report through `done`, never
        // reject the handler.
        let url = node.url;
        if (node.groups) {
          const target = new URL(url);
          target.searchParams.set('groups', node.groups);
          url = target.toString();
        }

        const readings = await fetchTelemetry(url, node.vrfName, node.timeoutMs);
        if (!Array.isArray(readings)) {
          throw new Error('unexpected payload: expected an array of readings');
        }
        msg.payload = readings;
        node.status({ fill: 'green', shape: 'dot', text: `${readings.length} values` });
        send(msg);
        done();
      } catch (err) {
        node.status({ fill: 'red', shape: 'ring', text: String(err.message).slice(0, 40) });
        done(err);
      }
    });

    node.on('close', () => {
      node.status({});
    });
  }

  RED.nodes.registerType('sevio-internal', SevioInternalNode);
};
