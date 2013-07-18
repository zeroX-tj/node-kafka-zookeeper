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
};

var onConsumerOffsets = function(offsets, error) {
  if (error) {
    console.error('onConsumerOffsets', error);
    return;
  }

  console.log('Offsets', offsets);
};

var onInitializeConsumerOffsets = function(error) {
  if (error) return console.error('onInitializeConsumerOffsets', error);
  console.log('Consumer offsets initialized');
};

zk.consumeTopic('MessageHeaders', 'dcrouse', onMessages);
zk.initializeConsumerOffsets('MessageHeaders', 'dcrouse', onInitializeConsumerOffsets);

//setInterval(function() {
//  zk.getConsumerOffsets('MessageHeaders', 'dcrouse', onConsumerOffsets);
//}, 5000);

