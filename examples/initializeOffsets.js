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

var Zookeeper = require('../lib/Zookeeper');
var Logger = require('../lib/Logger');

var zk = new Zookeeper({
    host: argv.host,
    port: argv.port,
    loglevel: argv.loglevel,
    logfile: argv.logfile
});

var log = new Logger(argv.loglevel, argv.logfile);

var onInitializeConsumerOffsets = function(error) {
  if (error) return log.error('onInitializeConsumerOffsets', error);
  log.info('Consumer offsets initialized');
};

zk.initializeConsumerOffsets(argv.topic, argv.group, onInitializeConsumerOffsets);

