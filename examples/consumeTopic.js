var Zookeeper = require('../lib/Zookeeper');
var Logger = require('../lib/Logger');

var zk = new Zookeeper({
  host: 'kafka00.lan',
  port: 2181,
  loglevel: 'info'
});

var log = new Logger('debug', 'Zookeeper.log');

var onMessages = function(messages, error, cb) {
  if (error) return log.error(error);
  log.info('Received %d messages', messages.length);
  //log.debug(messages[0]);

  // true  - (Acknowledge) Update Zk offsets and continue consuming
  // false - (Fail) Resend the same batch in 5 seconds so I don't
  //                have to put it somewhere. TODO: configure wait
  cb(true);
};

// Start consuming
// TODO: Support message filter function argument
zk.consumeTopic('MessageHeaders', 'dcrouse', onMessages);

// Stop consuming
// TODO: Implement

