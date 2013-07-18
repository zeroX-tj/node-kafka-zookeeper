var _ = require('underscore');
var bignum = require('bignum');
var zkcli = require('zookeeper');
var Consumer = require('prozess').Consumer;
var Logger = require('./Logger');
var log = new Logger('debug', 'Zookeeper.log');

var Zookeeper = function(options) {
  options = options || {};
  this.zkOpts = {
    connect: (options.host || 'localhost') + ':' + (options.port || 2181),
    timeout: options.timeout || 30000,
    debug_level: zkcli.ZOO_LOG_LEVEL_WARN,
    host_order_deterministic: false,
    data_as_buffer: false
  }
};

Zookeeper.prototype.consumeTopic = function(topic, group, cb) {
  var that = this;
  var consumers;

  var onGetConsumers = function(consumers, error) {
    log.debug('consumeTopic/onGetConsumers');
    if (error) return cb(null, 'Error retrieving consumers: ' + error);
    this.consumers = consumers;
    _.each(consumers, connectConsumer);
  }

  var connectConsumer = function(consumer) {
    log.info('Connecting consumer %s-%s...', consumer.host, consumer.partition);
    consumer.connect(function(error) { onConnectConsumer(consumer, error); });
  }

  var onConnectConsumer = function(consumer, error) {
    log.debug('consumeTopic/onConnectConsumer');
    if (error) return cb(null, 'Consumer connection error: ' + error);
    onConsumerReady(consumer);
  }

  var onConsumerReady = function(consumer) {
    log.debug('Consumer for %s-%s ready', consumer.host, consumer.partition);
    consumer.consume(function(error, messages) { onConsume(error, messages, consumer); });
  }

  var onConsume = function(error, messages, consumer) {
    log.debug('consumeTopic/onConsume');
    if (error) {
      if (error.message == 'OffsetOutOfRange') {
        log.info('Offset out of range; re-initializing');

        var onInitializeConsumerOffset = function(error) {
          if (error) return cb(null, 'Error retrieving consumer offset: ' + error);
          onConsumerReady(consumer);
        }

        that.initializeConsumerOffset(consumer, group, onInitializeConsumerOffset);
        return;
      } else {
        return cb(null, 'Unhandled error while consuming: ' + error);
      }
    }

    if (messages.length == 0) {
      log.debug('No messages');
      return setTimeout(function() { onConsumerReady(consumer) }, 2000);
    }

    var onAck = function(ack) {
      if (ack) {
        var incremental = _.chain(messages)
          .pluck('bytesLengthVal')
          .reduce(function(memo, num) { return memo + num; }, 0)
          .value();

        var newOffset = [{
          broker: _.chain(this.consumers).where({ host: consumer.host }).first().value().id,
          partition: consumer.partition,
          offset: bignum(consumer.offset).toString()
        }];

        var onSetConsumerOffsets = function(error) {
          if (error) return cb(null, error);
          if (error) log.error('WTF SHOULD HAVE RETURNED');
          onConsumerReady(consumer);
        }

        that.setConsumerOffsets(topic, group, newOffset, onSetConsumerOffsets);
      } else {
        log.warn('Messages failed by client; retrying in 5 seconds...');
        setTimeout(function() { onConsume(error, messages); }, 5000);
      }
    }

    cb(messages, error, onAck);
  }

  this.getConsumers(topic, group, onGetConsumers);
}

Zookeeper.prototype.getConsumers = function(topic, group, cb) {
  log.debug('getConsumers');
  var that = this;
  var brokers = [], consumers = [];
  var onConsumersCreated;

  var onGetBrokers = function(brokers, error) {
    log.debug('getConsumers/onGetBrokers');
    if (error) return cb(null, 'Error retrieving brokers: ' + error);
    this.brokers = brokers;
    that.getConsumerOffsets(topic, group, onGetConsumerOffsets);
  };

  // offsets: [{
  //   broker: '<int>',
  //   partition: '<int>',
  //   offset: '<bignum>'
  // }, ...]
  var onGetConsumerOffsets = function(offsets, error) {
    log.debug('getConsumers/onGetConsumerOffsets');
    if (error) return cb(null, 'Error retrieving consumer offsets: ' + error);

    // Create zero offsets for each broker/partition if none exist
    if (_.isEmpty(offsets)) {
      that.initializeConsumerOffsets(topic, group, brokers, function(error) {
        if (error) return cb(null, 'Error initializing offsets: ' + error);
        onGetBrokers(this.brokers);
      });

      return;
    }

    // Now that we know how many consumers to expect, set up the
    // completion function
    this.onConsumersCreated = _.after(offsets.length, function() {
      log.debug('getConsumers/onConsumersCreated');
      // Don't cause confusion downstream with an empty array
      if (_.isEmpty(consumers)) {
        return cb(null, 'Error creating consumers; collection is empty');
      }

      // Return the array of Consumers
      cb(consumers);
    });

    _.each(offsets, createConsumerOptions);
  };

  var createConsumerOptions = function(offset) {
    log.debug('getConsumers/createConsumerOptions');
    // Select approprate broker for this offset and prepare for extension
    var broker = {
      'broker': _.chain(this.brokers)
        .where({ id : offset.broker }) // Pull remaining broker data from
        .first().value()               // broker collection
    };

    var options = _.chain(offset)
      .omit('broker') // Remove broker id
      .extend(broker) // Add entire broker metadata object
      .value();

    createConsumer(options);
  };

  var createConsumer = function(options) {
    log.debug('getConsumers/createConsumer');
    var consumer = new Consumer({
        host: options.broker.host,
        port: options.broker.port,
        topic: topic,
        partition: options.partition,
        offset: new bignum(options.offset)
    });

    // Add broker id to the Consumer object for later
    // and add finished Consumer to the result array
    consumer.id = options.broker.id;
    consumers.push(consumer);
    this.onConsumersCreated();
  };

  this.getBrokers(onGetBrokers);
};

Zookeeper.prototype.getBrokers = function(cb) {
  log.debug('getBrokers');
  var that = this;
  var result = [], errors = [];
  var brokerIdPath = '/brokers/ids';
  var onBrokersProcessed;

  var onZkConnect = function(error) {
    log.debug('getBrokers/onZkConnect');
    if (error) return cb(null, 'Zk connect failed: ' + error);
    zk.a_get_children(brokerIdPath, false, onGetBrokers);
  };

  var onGetBrokers = function(rc, error, children) {
    log.debug('getBrokers/onGetBrokers');
    if (rc != 0) return cb(null, 'Get brokers failed: ' + error);

    // Now that we know how many brokers to expect, create the completion function
    that.onBrokersProcessed = _.after(children.length, function(errors) {
      log.debug('getBrokers/onBrokersProcessed');
      return cb(result, (errors.length > 0) ? errors : null);
    });

    (children.length == 0) ? that.onBrokersProcessed('No brokers') : _.each(children, getBroker);
  };

  var getBroker = function(brokerId) {
    log.debug('getBrokers/getBroker');
    var brokerPath = brokerIdPath + '/' + brokerId;
    zk.a_get(brokerPath, false, function(rc, error, stat, data) { onGetBroker(rc, error, data, brokerId); });
  };

  var onGetBroker = function(rc, error, data, brokerId) {
    log.debug('getBrokers/onGetBroker');
    if (rc != 0) {
      errors.push('Zookeeper broker query failed: ' + error);
    } else {
      var brokerKeys = ['id', 'name', 'host', 'port'];
      var brokerData = _.union(brokerId, data.split(':'));
      var broker = _.object(brokerKeys, brokerData);
      result.push(broker);
    }

    that.onBrokersProcessed(errors);
  };

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.getConsumerOffsets = function(topic, group, cb) {
  log.debug('getConsumerOffsets');
  var result = [], errors = [];
  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;
  var onOffsetsProcessed;

  var onZkConnect = function(error) {
    log.debug('getConsumerOffsets/onZkConnect');
    if (error) return cb(null, 'Zk connect failed: ' + error);
    zk.mkdirp(groupTopicPath, onGroupTopicPathCreated);
  };

  var onGroupTopicPathCreated = function(error) {
    log.debug('getConsumerOffsets/onGroupTopicPathCreated');
    if (error) return cb(null, 'Error creating group/topic path: ' + error);
    zk.a_get_children(groupTopicPath, false, onGetBrokerPartitions);
  };

  var onGetBrokerPartitions = function(rc, error, children) {
    log.debug('getConsumerOffsets/onGetBrokerPartitions');
    if (rc != 0) return cb(null, 'Get broker partitions failed: ' + error);

    // Now that we know how many offsets to expect, create the completion function
    this.onOffsetsProcessed = _.after(children.length, function(errors) {
      return cb(result, (errors && errors.length > 0) ? errors : null);
    });

    (children.length == 0) ? this.onOffsetsProcessed('No partitions') : _.each(children, getBrokerPartition);
  };

  var getBrokerPartition = function(brokerPartitionString) {
    log.debug('getConsumerOffsets/getBrokerPartition');
    var brokerPartitionPath = groupTopicPath + '/' + brokerPartitionString;
    zk.a_get(brokerPartitionPath, false, function(rc, error, stat, data) {
      onGetBrokerPartition(rc, error, data, brokerPartitionString);
    });
  };

  var onGetBrokerPartition = function(rc, error, data, brokerPartitionString) {
    log.debug('getConsumerOffsets/onGetBrokerPartition');
    if (rc == 0) {
      var brokerPartitionKeys = ['broker', 'partition'];
      var brokerPartition = _.object(brokerPartitionKeys, brokerPartitionString.split('-'));

      result.push({
          broker: brokerPartition['broker'],
          partition: brokerPartition['partition'],
          offset: data
      });
    } else {
      errors.push(error);
    }

    this.onOffsetsProcessed(errors);
  };

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.setConsumerOffsets = function(topic, group, offsets, cb) {
  log.debug('setConsumerOffsets');
  if (!offsets || _.isEmpty(offsets)) return cb('No offsets provided');
  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;

  var onZkConnect = function(error) {
    log.debug('setConsumerOffsets/onZkConnect');
    if (error) return cb(null, 'Zk connect failed: ' + error);
    // Ensure the group/topic path exists
    zk.mkdirp(groupTopicPath, onGroupTopicPathCreated);
  };

  var onGroupTopicPathCreated = function(error) {
    log.debug('setConsumerOffsets/onGroupTopicPathCreated');
    if (error) return cb('Error creating group/topic path: ' + error);
    _.each(offsets, getOffset);
  };

  var getOffset = function(offset) {
    log.debug('setConsumerOffsets/getOffset');
    var offsetPath = groupTopicPath + '/' + offset.broker + '-' + offset.partition;
    zk.a_exists(offsetPath, false, function(rc, error, stat) {
      if (rc != 0) {
        if (error == 'no node') {
          zk.a_create(offsetPath, offset, null, onOffsetProcessed);
        } else {
          return cb('Error retriving offset: ' + error);
        }
      } else {
        zk.a_set(offsetPath, offset.offset, stat.version, onOffsetProcessed);
      }
    });
  };

  var onOffsetProcessed = function(rc, error) {
    log.debug('setConsumerOffsets/onOffsetProcessed');
    if (rc != 0) log.error('Error processing offset: %s', error);
    onOffsetsProcessed();
  };

  var onOffsetsProcessed = _.after(_.keys(offsets).length, function() {
    return cb();
  });

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.initializeConsumerOffset = function(consumer, group, cb) {
  var that = this;

  var onGetLatestOffset = function(error, offset) {
    if (error) return cb(null, 'Error initializing offset: ' + error);
    var newOffset = [{
        broker: consumer.id,
        partition: consumer.partition,
        offset: offset.toString()
    }];

    that.setConsumerOffsets(consumer.topic, group, newOffset, onSetConsumerOffsets);
  };

  var onSetConsumerOffsets = function(error) {
    if (error) return cb(null, new Error('Error initializing consumer offset: ' + error));
    cb(consumer);
  };

  consumer.getEarliestOffset(onGetLatestOffset);
};

Zookeeper.prototype.initializeConsumerOffsets = function(topic, group, cb) {
  var that = this;
  var partitionCount;

  var onZkConnect = function(error) {
    if (error) return cb(new Error('Zookeeper connection error: ' + error));
    that.getBrokers(onGetBrokers);
  };

  var onGetBrokers = function(brokers, error) {
    if (error) return cb(new Error('Error retrieving brokers: ' + error));
    _.each(this.brokers, initializeBrokerOffset);
  };

  var initializeBrokerOffset = function(broker) {
    that.getTopicBrokerPartitions(topic, broker, onGetTopicBrokerPartitions);
  };

  var onGetTopicBrokerPartitions = function(topic, broker, partitions, error) {
    if (error) return cb(new Error('Error retrieving topic/broker partitions: ' + error));
    partitionCount = partitions;
    _.times(partitions, function(partition) { initializePartition(partition, broker); });
  };

  var initializePartition = function(partition, broker) {
    var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;
    var offsetPath = groupTopicPath + '/' + broker.id + '-' + partition;
    log.info('Initializing offset: %s', offsetPath);
    zk.a_create(offsetPath, '0', null, onInitializePartition);
  };

  var onInitializePartition = _.after(partitionCount, cb());

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

Zookeeper.prototype.getTopicBrokerPartitions = function(topic, broker, cb) {
  var topicBrokerPath = '/brokers/topics/' + topic + '/' + broker.id;

  var onZkConnect = function(error) {
    if (error) return cb(topic, broker, null, 'Zookeeper connection error: ' + error);
    zk.a_get(topicBrokerPath, false, onGetPartitions);
  };

  var onGetPartitions = function(rc, error, stat, data) {
    if (rc != 0 ) return cb(topic, broker, null, 'Error retrieving topic paritions: ' + error);
    return cb(topic, broker, Number(data));
  };

  var zk = new zkcli(this.zkOpts);
  zk.connect(onZkConnect);
};

module.exports = Zookeeper;

