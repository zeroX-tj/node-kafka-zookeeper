var argv = require('optimist')
.usage('Usage: $0 [options]')
.describe({
    host: 'Elastic Search host',
    port: 'Elastic Search port',
    topic: 'Kafka topic',
    group: 'Kafka consumer group',
    loglevel: 'Log verbosity',
    logfile: 'Log file path'
})
.default({
    host: 'kafka00.lan',
    port: 2181,
    topic: 'MessageHeaders',
    group: 'default',
    loglevel: 'info',
    logfile: 'Zookeeper.log'
})
.argv;

var _ = require('underscore');
var Zookeeper = require('../lib/Zookeeper');
var Logger = require('../lib/Logger');
var loglevel = 'info';

var zk = new Zookeeper({
    host: argv.host,
    port: argv.port,
    loglevel: argv.loglevel
});

var log = new Logger(argv.loglevel, argv.logfile);

var lastOffsets;

var getConsumerOffsets = function() {
  zk.getConsumerOffsets(argv.topic, argv.group, onConsumerOffsets);
};

var onConsumerOffsets = function(error, offsets) {
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

