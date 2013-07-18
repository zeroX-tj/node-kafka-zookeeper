var _ = require('underscore');
var Zookeeper = require('../lib/Zookeeper');
var Logger = require('../lib/Logger');
var loglevel = 'info';

var zk = new Zookeeper({
    host: 'kafka00.lan',
    port: 2181,
    loglevel: loglevel
});

var log = new Logger(loglevel, 'Zookeeper.log');

var lastOffsets;

var getConsumerOffsets = function() {
  zk.getConsumerOffsets('MessageHeaders', 'dcrouse', onConsumerOffsets);
};

var onConsumerOffsets = function(offsets, error) {
  if (error) return log.error('onConsumerOffsets', error);

  if (!lastOffsets || !_.isEqual(lastOffsets, offsets)) {
    log.info(offsets);
    lastOffsets = offsets;
  } else {
    log.debug(offsets);
  }
};

setInterval(getConsumerOffsets, 5000);
getConsumerOffsets();

