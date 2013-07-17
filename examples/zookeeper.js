var Zookeeper = require('../lib/Zookeeper');

var zk = new Zookeeper({
    host: 'kafka00.lan',
    port: 2181
});

var onMessages = function(messages, error, cb) {
  if (error) return console.error(error);
  console.log('Received %d messages', messages.length);

  // Acknowlege to update Zk offsets and continue consuming
  cb(true);

  // Or fail to resend the same batch in 5 seconds
  // cb(false);
}

zk.consumeTopic('MessageHeaders', 'dcrouse', onMessages);

