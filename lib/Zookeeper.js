var _ = require('underscore');
var bignum = require('bignum');
var zkcli = require('zookeeper');
var Consumer = require('prozess').Consumer;

var Zookeeper = function(options) {
  options = options || {};
  this.zkOpts = {
    connect: (options.host || 'localhost') + ':' + (options.port || 2181),
    timeout: options.timeout || 30000,
    debug_level: Zookeeper.ZOO_LOG_LEVEL_WARN,
    host_order_deterministic: false,
    data_as_buffer: false
  }
};

Zookeeper.prototype.consumeTopic = function(topic, group, cb) {
  var that = this;
  var consumers;

  var onGetConsumers = function(consumers, error) {
    if (error) return cb(null, 'Error retrieving consumers: ' + error);
    this.consumers = consumers;
    console.log('Connecting...');
    _.each(consumers, function(consumer) {
      consumer.connect(function(error) {
        if (error) return cb(null, 'Consumer connection error: ' + error);
        onConsumerReady(consumer);
      });
    });
  }

  var onConsumerReady = function(consumer) {
    consumer.consume(function(error, messages) { onConsume(error, messages, consumer); });
  }

  var onConsume = function(error, messages, consumer) {
    if (error && error.message == 'OffsetOutOfRange') {
      that.initializeConsumerOffset(consumer, group, function() {
        onConsumerReady(consumer);
      });

      return;
    }

    if (messages.length == 0) {
      return setTimeout(function() { onConsumerReady(consumer) }, 2000);
    }

    cb(messages, error, function(ack) {
      if (ack) {
        var incremental = _.chain(messages)
          .pluck('bytesLengthVal')
          .reduce(function(memo, num) { return memo + num; }, 0)
          .value();

        var newOffset = [{
          broker: _.chain(this.consumers).where({ host: consumer.host }).first().value().id,
          partition: consumer.partition,
          offset: bignum(consumer.offset).add(incremental).toString()
        }];

        that.setConsumerOffsets(topic, group, newOffset, function(error) {
          if (error) return cb(null, error);
          onConsumerReady(consumer);
        });
      } else {
        console.error('Messages failed by client; retrying in 5 seconds...');
        setTimeout(function() { onConsume(error, messages); }, 5000);
      }
    });
  }

  this.getConsumers(topic, group, onGetConsumers);
}

Zookeeper.prototype.getConsumers = function(topic, group, cb) {
  var that = this;
  var brokers;

  var onGetBrokers = function(brokers, error) {
    if (error) return cb(null, 'Error retrieving brokers: ' + error);
    this.brokers = brokers;
    that.getConsumerOffsets(topic, group, onGetConsumerOffsets);
  }

  // offsets: {
  //   broker: '<int>',
  //   partition: '<int>',
  //   offset: '<bignum>'
  // }
  var onGetConsumerOffsets = function(offsets, error) {
    if (error) return cb(null, 'Error retrieving consumer offsets: ' + error);

    // Create zero offsets for each broker/partition if none exist
    if (_.isEmpty(offsets)) {
      that.initializeConsumerOffsets(topic, group, brokers, function(error) {
        if (error) return cb(null, 'Error initializing offsets: ' + error);
        onGetBrokers(this.brokers);
      });

      return;
    }

    var options = [];
    _.each(offsets, function(offset) {
      // Select approprate broker for this offset and prepare for extension
      var broker = {
        'broker': _.chain(this.brokers)
          .where({ id : offset.broker }) // Pull remaining broker data from
          .first().value()               // broker collection
      };

      // Add finished consumer options object to collection
      options.push(_.chain(offset)
        .omit('broker') // Remove broker id
        .extend(broker) // Add entire broker metadata object
        .value());
    });

    // Create a new Consumer from each options object in collection
    var consumers = [];
    _.each(options, function(option) {
      var consumer = new Consumer({
          host: option.broker.host,
          port: option.broker.port,
          topic: topic,
          partition: option.partition,
          offset: new bignum(option.offset)
      });

      // Add broker id to the Consumer object for later
      // and add finished Consumer to the result array
      consumer.id = option.broker.id;
      consumers.push(consumer);
    });

    // Don't cause confusion downstream with an empty array
    if (_.isEmpty(consumers)) {
      return cb(null, 'Error creating consumers; collection is empty');
    }

    // Return the array of Consumers
    return cb(consumers);
  }

  this.getBrokers(onGetBrokers);
};

Zookeeper.prototype.getBrokers = function(cb) {
  var that = this;
  var brokerIdPath = '/brokers/ids';

  var onZkConnect = function(error) {
    if (error) return cb(null, 'Zk connect failed: ' + error);
    zk.a_get_children(brokerIdPath, false, onGetBrokers);
  }

  var onGetBrokers = function(rc, error, children) {
    if (rc != 0) return cb(null, 'Get brokers failed: ' + error);

    var onBrokersProcessed = _.after(children.length, function(errors) {
      zk.close();
      return cb(result, (errors.length > 0) ? errors : null);
    });

    (children.length == 0) ? onBrokersProcessed() : null;

    var result = [], errors = [];
    _.each(children, function(broker) {
      var brokerPath = brokerIdPath + '/' + broker;
      zk.a_get(brokerPath, false, function(rc, error, stat, data) {
        if (rc == 0) {
          var brokerData = _.object(['id', 'name', 'host', 'port'], _.union(broker, data.split(':')));
          result.push(brokerData)
        } else {
          errors.push('Zookeeper broker query failed: ' + error);
        }

        onBrokersProcessed(errors);
      });
    });
  }

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.getConsumerOffsets = function(topic, group, cb) {
  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;

  var onZkConnect = function(error) {
    if (error) return cb(null, 'Zk connect failed: ' + error);
    zk.mkdirp(groupTopicPath, onGroupTopicPathCreated);
  }

  var onGroupTopicPathCreated = function(error) {
    if (error) return cb(null, 'Error creating group/topic path: ' + error);
    zk.a_get_children(groupTopicPath, false, onGetBrokerPartitions);
  }

  var onGetBrokerPartitions = function(rc, error, children) {
    if (rc != 0) return cb(null, 'Get broker partitions failed: ' + error);
    var result = [], errors = [];

    var onOffsetsProcessed = _.after(children.length, function(errors) {
      zk.close();
      return cb(result, (errors && errors.length > 0) ? errors : null);
    });

    (children.length == 0) ? onOffsetsProcessed() : null;

    _.each(children, function(brokerPartition) {
      var brokerPartitionPath = groupTopicPath + '/' + brokerPartition;
      zk.a_get(brokerPartitionPath, false, function(rc, error, stat, data) {
        if (rc == 0) {
          var brokerPartitionData = _.object(['broker', 'partition'], brokerPartition.split('-'));
          result.push({
              broker: brokerPartitionData['broker'],
              partition: brokerPartitionData['partition'],
              offset: data
          });
        } else {
          errors.push(error);
        }
        onOffsetsProcessed(errors);
      });
    });
  }

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.setConsumerOffsets = function(topic, group, offsets, cb) {
  if (!offsets || _.isEmpty(offsets)) return cb('No offsets provided');
  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;

  var onZkConnect = function(error) {
    if (error) return cb(null, 'Zk connect failed: ' + error);
    // Ensure the group/topic path exists
    zk.mkdirp(groupTopicPath, onGroupTopicPathCreated);
  }

  var onGroupTopicPathCreated = function(error) {
    if (error) return cb('Error creating group/topic path: ' + error);
    _.each(offsets, processOffset);
  }

  var processOffset = function(offset) {
    var offsetPath = groupTopicPath + '/' + offset.broker + '-' + offset.partition;
    //console.log('Processing offset for %s', offsetPath);

    // Create/update the offset in Zookeeper accordingly
    zk.a_exists(offsetPath, false, function(rc, error, stat) {
      if (rc != 0) {
        if (error == 'no node') {
          zk.a_create(offsetPath, offset, null, onOffsetProcessed);
        } else {
          return console.error('Zk query on %s failed: %s', offsetPath, error);
        }
      } else {
        zk.a_set(offsetPath, offset.offset, stat.version, onOffsetProcessed);
      }
    });
  }

  var onOffsetProcessed = function(rc, error) {
    if (rc != 0) {
      console.error('Error processing offset: %s', error);
    }

    onOffsetsProcessed();
  }

  var onOffsetsProcessed = _.after(_.keys(offsets).length, function() {
    zk.close();
    return cb();
  });

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.initializeConsumerOffset = function(consumer, group, cb) {
  var that = this;

  var onGetLatestOffset = function(error, offset) {
    if (error) return console.error('Error initializing offset: %s', error);
    var newOffset = [{
        broker: consumer.id,
        partition: consumer.partition,
        offset: offset.toString()
    }];

    that.setConsumerOffsets(consumer.topic, group, newOffset, onSetConsumerOffsets);
  }

  var onSetConsumerOffsets = function(error) {
    if (error) return cb(null, 'Error initializing consumer offset: ' + error);
    cb(consumer);
  }

  consumer.getLatestOffset(onGetLatestOffset);
}

Zookeeper.prototype.initializeConsumerOffsets = function(topic, group, brokers, cb) {
  var that = this;

  var onZkConnect = function(error) {
    if (error) return cb(topic, broker, null, 'Zookeeper connection error: ' + error);
    _.each(this.brokers, initializeBrokerOffset);
  }

  var initializeBrokerOffset = function(broker) {
    that.getTopicBrokerPartitions(topic, broker, onGetTopicBrokerPartitions);
  }

  var partitionCount;
  var onGetTopicBrokerPartitions = function(topic, broker, partitions, error) {
    if (error) return cb(null, 'Error retrieving topic/broker partitions: ' + error);
    partitionCount = partitions;
    _.times(partitions, function(partition) {
      initializePartition(partition, broker);
    });
  }

  var initializePartition = function(partition, broker) {
    var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;
    var offsetPath = groupTopicPath + '/' + broker.id + '-' + partition;
    console.log('Initializing offset: %s', offsetPath);
    zk.a_create(offsetPath, '0', null, onInitializePartition);
  }

  var onInitializePartition = _.after(partitionCount, function() {
    cb();
  });

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
}

Zookeeper.prototype.getTopicBrokerPartitions = function(topic, broker, cb) {
  var topicBrokerPath = '/brokers/topics/' + topic + '/' + broker.id;

  var onZkConnect = function(error) {
    if (error) return cb(topic, broker, null, 'Zookeeper connection error: ' + error);
    zk.a_get(topicBrokerPath, false, onGetPartitions);
  }

  var onGetPartitions = function(rc, error, stat, data) {
    if (rc != 0 ) return cb(topic, broker, null, 'Error retrieving topic paritions: ' + error);
    return cb(topic, broker, Number(data));
  }

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
}

module.exports = Zookeeper;

