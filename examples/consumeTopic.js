var argv = require('optimist')
.usage('Usage: $0 [options]')
.describe({
    host: 'Zookeeper host',
    port: 'Zookeeper port',
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
var Logger = require('Logger');
var _ = require('underscore');

var zk = new Zookeeper({
    host: argv.host,
    port: argv.port,
    loglevel: argv.loglevel,
    logfile: argv.logfile
});

var log = new Logger(argv.loglevel, argv.logfile);

var onMessages = function(error, messages, cb) {
  if (error) return log.error(error);
  log.info('Received %d messages', messages.length);
  log.debug(messages[0].substring(0, 100) + '...');

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

