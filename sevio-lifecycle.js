module.exports = (RED) => {
  function SevioLifecycleNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    // Cache the device node reference at construction time.
    // Re-fetching at close time is unreliable during concurrent close
    // (Node-RED stops nodes via Promise.all — the device node may already
    // be partially destroyed, making brokerConn.createMsg unavailable).
    const deviceNode = config.deviceNodeId ? RED.nodes.getNode(config.deviceNodeId) : null;

    // Close handler: publishes DDEATH on flow delete or disable.
    // Calls sendDDeath() synchronously (no callback) to complete before
    // the broker node's client.end() sets disconnecting=true, which would
    // block the MQTT publish via _checkDisconnecting.
    //
    // WARNING: This depends on MQTT.js QoS 0 publish being synchronous
    // (writes to stream before returning). Verified in mqtt@4.x and mqtt@5.x.
    // If upgrading MQTT.js, verify client.publish() still writes synchronously
    // for QoS 0, or refactor to use a broker pre-close hook.
    //
    // Single-flow case: NDEATH already signals Edge Node death — DDEATH
    // is best-effort here; if the broker is already disconnecting, it
    // silently fails (acceptable per spec).
    node.on('close', function (removed, done) {
      if (deviceNode) {
        if (deviceNode.brokerConn && typeof deviceNode.brokerConn.createMsg === 'function') {
          // Fire-and-forget: don't wait for the publish callback.
          // This avoids the race where client.end() sets disconnecting=true
          // before the publish callback fires.
          deviceNode.sendDDeath();
        }
      }
      // Always complete immediately — don't block Node-RED close cycle.
      done();
    });
  }

  RED.nodes.registerType('sevio-lifecycle', SevioLifecycleNode);
};
