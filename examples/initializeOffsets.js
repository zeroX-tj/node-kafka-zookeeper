var Zookeeper = require('../lib/Zookeeper');
var Logger = require('../lib/Logger');

var zk = new Zookeeper({
    host: 'kafka00.lan',
    port: 2181
});

var log = new Logger('debug', 'Zookeeper.log');

var onInitializeConsumerOffsets = function(error) {
  if (error) return log.error('onInitializeConsumerOffsets', error);
  log.info('Consumer offsets initialized');
};

zk.initializeConsumerOffsets('MessageHeaders', 'dcrouse', onInitializeConsumerOffsets);

