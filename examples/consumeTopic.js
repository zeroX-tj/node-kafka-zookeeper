var Kafkazoo = require('../lib/Kafkazoo');
var Logger = require('Logger');
var _ = require('underscore');

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

// when there are messages to process...
var onMessages = function (error, messages, acknowledge) {
    if (error) return log.error(error);
    // log some details
    log.info('Received %d messages', messages.length);
    log.debug(messages[0].substring(0, 100) + '...');

    // and get next batch
    acknowledge(true); // false will resend the same messages after a delay
};

// Start consuming
kafka.consume(argv.topic, argv.group, onMessages);

// Stop consuming
// not yet implemented
