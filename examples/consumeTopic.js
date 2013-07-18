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
var _ = require('underscore');
var zlib = require('zlib');

var zk = new Zookeeper({
    host: argv.host,
    port: argv.port,
    loglevel: argv.loglevel,
    logfile: argv.logfile
});

var log = new Logger(argv.loglevel, argv.logfile);

var printSample = function(message) {
  //console.log(message);

  zlib.gunzip(message.payload, function(error, buffer) {
    if (error) return log.error(error);
    //log.info(buffer.toString('utf8'));
  });
};

var onMessages = function(messages, error, cb) {
  if (error) return log.error(error);
  log.info('Received %d messages', messages.length);
  _.each(messages, printSample);

  // true  - (Acknowledge) Update Zk offsets and continue consuming
  // false - (Fail) Resend the same batch in 5 seconds so I don't
  //                have to put it somewhere. TODO: configure wait
  cb(true);
};

// Start consuming
// TODO: Support message filter function argument
zk.consumeTopic(argv.topic, argv.group, onMessages);

// Stop consuming
// TODO: Implement

