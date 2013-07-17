node-kafka-zookeeper
=======

A high-level client library in Node.js for the Apache Kafka project with Zookeeper integration

[Kafka](http://incubator.apache.org/kafka/index.html) is a persistent, efficient, distributed publish/subscribe messaging system.

##Zookeeper consumer example:

A `Zookeeper` object handles broker enumeration and offset storage
```javascript
var Zookeeper = require('kafka-zookeeper').Zookeeper;
var zk = new Zookeeper({
  host: 'kafka00.lan',
  port: 2181
});

var onMessages = function(messages, error, cb) {
  if (error) return console.error(error);
  console.log('Received %d messages', messages.length);

  // true  - (Acknowledge) Update Zk offsets and continue consuming
  // false - (Fail) Resend the same batch in 5 seconds so I don't
  //                have to put it somewhere. TODO: configure wait
  cb(true);
}

// Start consuming
// TODO: Support message filter function argument
zk.consumeTopic('MessageHeaders', 'dcrouse', onMessages);

// Stop consuming
// TODO: Implement

```

##Installation:

  npm install kafka-zookeeper

##Checkout the code and run the tests:

  git clone https://github.com/devoncrouse/node-kafka-zookeeper.git
  cd node-kafka-zookeeper; make test-cov && open coverage.html

##Kafka Compatability matrix:

<table>
  <tr>
     <td>Kakfa 0.8.0 Release</td><td>Not Supported</td>
  </tr>
  <tr>
    <td>Kafka 0.7.2 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.1 Release</td><td>Supported</td>
  <tr>
    <td>Kafka 0.7.0 Release</td><td>Supported</td>
  <tr>
    <td>kafka-0.6</td><td>Consumer-only support.</td>
  <tr>
    <td>kafka-0.05</td><td>Not Supported</td>
</table>

Versions taken from http://incubator.apache.org/kafka/downloads.html
