module.exports = (RED) => {
  const fs = require('fs');
  const net = require('net');

  const RECIPIENTS_FILE = '/tmp/datagateway-alert-recipients.json';
  // ubus is exposed to local processes through the JSON-RPC proxy
  // (`ubus-json-server`) — `/var/run/ubus.sock` does not exist on the device.
  // Mirrors datagateway-api's UBUS_SOCKET_PATH default.
  const UBUS_SOCKET = '/var/run/ubus-json.sock';
  // rpcd's own ubus call timeout is 30 s (a roaming SMSC ack can be slow);
  // leave margin so the server can answer before we give up.
  const UBUS_TIMEOUT_MS = 35000;

  const failure = (code, message) => {
    const err = new Error(message);
    err.code = code;
    return err;
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
   * Calls a ubus object over the JSON-RPC UNIX socket, mirroring
   * datagateway-api/src/clients/ubusClient.ts:
   *   → {"jsonrpc":"2.0","method":"call","params":["object","method",{data}]}\n
   *   ← {"jsonrpc":"2.0","result":[0, values]}
   * Returns the socket so the node can destroy in-flight calls on close.
   */
  const callUbus = (socketPath, object, method, data, callback) => {
    const client = new net.Socket();
    const chunks = [];
    let done = false;
    let timer = null;

    const finish = (err, value) => {
      if (done) return;
      done = true;
      if (timer) clearTimeout(timer);
      client.removeAllListeners();
      client.destroy();
      callback(err, value);
    };

    const parse = () => {
      let raw;
      try {
        raw = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      } catch (parseErr) {
        return false; // incomplete response — wait for more data
      }
      if (!raw || typeof raw !== 'object' || !('jsonrpc' in raw)) {
        finish(failure('ubus_error', 'ubus: invalid JSON-RPC response'));
        return true;
      }
      if (raw.error) {
        finish(failure('ubus_error', `ubus error: ${JSON.stringify(raw.error)}`));
        return true;
      }
      const result = raw.result;
      if (!Array.isArray(result) || result.length === 0) {
        finish(failure('ubus_error', 'ubus: empty response'));
        return true;
      }
      if (typeof result[0] === 'number') {
        if (result[0] !== 0) {
          finish(failure('ubus_error', `ubus call failed: ${JSON.stringify(result)}`));
        } else {
          finish(null, result[1]);
        }
      } else {
        // No status code — treat the first element as the return value.
        finish(null, result[0]);
      }
      return true;
    };

    client.on('data', (chunk) => {
      chunks.push(chunk);
      parse();
    });

    client.on('error', (err) => {
      finish(failure('ubus_error', `ubus: ${err.message}`));
    });

    client.on('close', () => {
      if (done) return;
      if (!parse()) {
        finish(failure('ubus_error', 'ubus: invalid JSON response'));
      }
    });

    timer = setTimeout(() => {
      finish(failure('timeout', `ubus: response timeout (${UBUS_TIMEOUT_MS}ms)`));
    }, UBUS_TIMEOUT_MS);

    client.connect(socketPath, () => {
      const message = {
        jsonrpc: '2.0',
        method: 'call',
        params: [object, method, data]
      };
      client.end(JSON.stringify(message) + '\n');
    });

    return client;
  };

  function SevioSmsNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    // Overridable in tests (`config.*`) and in the dev harness
    // (`SEVIO_SMS_UBUS_SOCKET`); the production paths are fixed by the contract.
    const recipientsFile = config.recipientsFile || RECIPIENTS_FILE;
    const ubusSocket = config.ubusSocket || process.env.SEVIO_SMS_UBUS_SOCKET || UBUS_SOCKET;
    const sockets = new Set();
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

          const socket = callUbus(ubusSocket, 'sms', 'send', { to: [to[index]], body: body }, (ubusErr, result) => {
            sockets.delete(socket);
            if (closed) return;
            if (ubusErr) {
              fail(ubusErr.code, ubusErr.message);
              return;
            }

            const reply = result && typeof result === 'object' ? result : {};
            if (reply.ok) {
              if (reply.ref !== undefined && reply.ref !== null) {
                refs.push(reply.ref);
              }
              sendNext(index + 1);
            } else {
              fail(reply.code || 'send_failed', reply.message || 'sms send failed');
            }
          });
          sockets.add(socket);
        };

        sendNext(0);
      });
    });

    node.on('close', function (removed, done) {
      closed = true;
      for (const socket of sockets) {
        socket.destroy();
      }
      sockets.clear();
      done();
    });
  }

  RED.nodes.registerType('sevio-sms', SevioSmsNode);
};
