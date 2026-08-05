module.exports = (RED) => {
  const loadFile = (filePath, node) => {
    // Load the configuration from the specified file
    const fs = require('fs');
    const path = require('path');
    const configPath = path.resolve(__dirname, filePath);

    fs.readFile(configPath, 'utf8', (err, data) => {
      if (err) {
        node.error(`Error reading config file: ${err.message}`);
        return;
      }
      try {
        let configData = JSON.parse(data);
        node.status({ fill: 'green', shape: 'dot', text: 'loaded' });
        node.send({ payload: configData });
      } catch (parseErr) {
        node.status({ fill: 'red', shape: 'dot', text: 'error' });
        node.error(`Error parsing config file: ${parseErr.message}`);
      }
    });
  };
  function ConfigLoadNode(config) {
    RED.nodes.createNode(this, config);
    let node = this;

    node.status({ fill: 'blue', shape: 'dot', text: 'ready' });

    node.on('input', (_) => {
      loadFile(config.configFilepath, node);
      node.error('INPUT EVENT');
    });

    loadFile(config.configFilepath, node);
  }

  RED.nodes.registerType('config-load', ConfigLoadNode);
};
