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

// when offsets is initialized...
var onInitializeConsumerOffsets = function (error) {
    if (error) return log.error('onInitializeConsumerOffsets', error);
    log.info('Consumer offsets initialized');
};

// do it
kafka.initializeConsumerOffsets(argv.topic, argv.group, onInitializeConsumerOffsets);

