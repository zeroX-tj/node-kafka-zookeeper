var _ = require('underscore');
var Kafkazoo = require('../lib/Kafkazoo');
var Logger = require('Logger');

// command line parameters
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
        host: 'localhost',
        port: 2181,
        topic: 'MessageHeaders',
        group: 'default',
        loglevel: 'info',
        logfile: 'Kafkazoo.log'
    })
    .argv;

// go log
var log = new Logger(argv.loglevel, argv.logfile);

// kafka client connected via zookeeper
var kafka = new Kafkazoo({
    host: argv.host,
    port: argv.port,
    logger: log
});

var lastOffsets;

// when offset information is received...
var onConsumerOffsets = function (error, offsets) {
    if (error) return log.error('onConsumerOffsets', error);

    if (!lastOffsets || !_.isEqual(lastOffsets, offsets)) {
        log.info(offsets);
        lastOffsets = offsets;
    } else {
        log.debug(offsets);
    }
};

// how to get the offset information
var getConsumerOffsets = function () {
    kafka.getConsumerOffsets(argv.topic, argv.group, onConsumerOffsets);
};

// do it
getConsumerOffsets();
// ...repeatedly
setInterval(getConsumerOffsets, 5000);
