var _ = require('underscore');
_.str = require('underscore.string');
_.mixin(_.str.exports());

var async = require('async');
var bignum = require('bignum');
var zlib = require('zlib');
var zkcli = require('zookeeper');
var Consumer = require('prozess').Consumer;
var Logger = require('./Logger');
var log, config, zkOpts;

var Zookeeper = function(options) {
  config = options || {};
  zkOpts = {
    connect: (config.host || 'localhost') + ':' + (config.port || 2181),
    timeout: config.timeout || 30000,
    debug_level: zkcli.ZOO_LOG_LEVEL_WARN,
    host_order_deterministic: false,
    data_as_buffer: false
  };

  log = new Logger(config.loglevel || 'warn', 'Zookeeper.log');
};

Zookeeper.prototype.consumeTopic = function(topic, group, cb) {
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  var that = this;
  var consumers;

  var seriesCallback = function(error) {
    log.debug('consumeTopic/seriesCallback');
    if (error) cb(error);
  };

  async.waterfall([
      function getConsumers(callback) {
        log.debug('consumeTopic/getConsumers');
        that.getConsumers(topic, group, function(error, result) { callback(error, result); });
      },
      function connectConsumers(consumers, callback) {
        log.debug('consumeTopic/connectConsumers');
        async.each(consumers, function(consumer, connectCallback) {
          log.info('Connecting consumer %s-%s...', consumer.host, consumer.partition);
          consumer.connect(function(error) { connectCallback(error); });
        }, function(error) {
          callback(error, consumers);
        });
      },
      function startConsuming(consumers, callback) {
        log.debug('consumeTopic/startConsuming');
        async.each(consumers, function(consumer, consumerCallback) {
          onConsumerReady(consumer);
          consumerCallback();
        });

        callback();
      }
  ], seriesCallback);

  var onConsumerReady = function(consumer) {
    log.debug('Consumer for %s-%s ready', consumer.host, consumer.partition);
    consumer.consume(function(error, messages) { onConsume(error, messages, consumer); });
  }

  var onConsume = function(error, messages, consumer) {
    log.debug('consumeTopic/onConsume');
    if (error) {
      if (error.message == 'OffsetOutOfRange') {
        log.info('Offset out of range; re-initializing');

        that.initializeConsumerOffset(consumer, group, function(error) {
          if (error) return cb('Error retrieving consumer offset: ' + error);
          onConsumerReady(consumer);
        });

        return;
      } else {
        return cb('Unhandled error while consuming: ' + error);
      }
    }

    that.decompressMessages(messages, function(error, result) {
      if (messages.length == 0) {
        log.debug('No messages');
        return setTimeout(function() { onConsumerReady(consumer) }, 10000);
      }

      var onAck = function(ack) {
        if (ack) {
          var newOffset = [{
            broker: consumer.id,
            partition: consumer.partition,
            offset: bignum(consumer.offset).toString()
          }];

          that.setConsumerOffsets(topic, group, newOffset, function(error) {
            if (error) return console.trace(error);
            onConsumerReady(consumer);
          });
        } else {
          log.warn('Messages failed by client; retrying in 5 seconds...');
          setTimeout(function() { onConsume(null, messages); }, 5000);
        }
      };

      cb(error, result, onAck);
    });
  }
};

Zookeeper.prototype.decompressMessages = function(messages, cb) {
  var result = [];

  async.each(messages, function(message, messageCallback) {
    switch (message.compression) {
      case 0:
        result.push(message.payload);
        break;
      case 1:
        zlib.gunzip(message.payload, function(error, buffer) {
          if (error) return messageCallback(error);

          while (buffer.length > 0) {
            var size = buffer.readUInt32BE(0) + 4;
            var msg = buffer.toString('utf8', 10, size);
            result.push(msg);
            buffer = buffer.slice(size);
          }

          return messageCallback();
        });
        break;
      case 2:
        messageCallback('Snappy not implemented');
        break;
      default:
        messageCallback('Unknown compression: ' + message.compression);
        break;
    }
  }, function(error) {
    cb(error, result);
  });
};

Zookeeper.prototype.getConsumers = function(topic, group, cb) {
  log.debug('getConsumers');
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  var that = this;

  var seriesCallback = function(error, result) {
    log.debug('getConsumers/seriesCallback');
    cb(error, result);
  };

  async.waterfall([
      function getTopicGroupData(callback) {
        log.debug('getConsumers/getTopicGroupData');
        async.parallel({
          getBrokers: function(callback) {
            log.debug('getConsumers/getTopicGroupData/getBrokers');
            that.getBrokers(function(error, result) { callback(error, result); });
          },
          getConsumerOffsets: function(callback) {
            log.debug('getConsumers/getTopicGroupData/getConsumerOffsets');
            that.getConsumerOffsets(topic, group, function(error, result) { callback(error, result); });
          }
        }, function(error, results) {
          callback(error, results.getBrokers, results.getConsumerOffsets);
        });
      },
      function initializeOffsetsConditional(brokers, offsets, callback) {
        // Create zero offsets for each broker/partition if none exist
        if (_.isEmpty(offsets)) {
          log.debug('getConsumers/initializeOffsetsConditional/initializeConsumerOffsets');
          that.initializeConsumerOffsets(topic, group, function(error, result) {
            if (error) return callback(error);
            callback(null, brokers, result);
          });
        } else {
          callback(null, brokers, offsets);
        }
      },
      function createConsumerOptions(brokers, offsets, callback) {
        var result = [];
        async.each(offsets, function(offset, offsetCallback) {
          // Select approprate broker for this offset and prepare for extension
          var broker = {
            'broker': _.chain(brokers)
              .where({ id : offset.broker }) // Pull remaining broker data from
              .first().value()               // broker collection
          };

          result.push(_.chain(offset)
            .omit('broker') // Remove broker id
            .extend(broker) // Add entire broker metadata object
            .value());

          offsetCallback();
        }, function(error) { callback(error, result); });
      },
      function createConsumers(optionsArray, callback) {
        var result = [];
        async.each(optionsArray, function(options, optionsCallback) {
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
          result.push(consumer);
          optionsCallback();
        }, function(error) { return callback(error, result); });
      }
  ], seriesCallback);
};

Zookeeper.prototype.getBrokers = function(cb) {
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  log.debug('getBrokers');
  var that = this, brokerIdPath = '/brokers/ids';
  var zk = new zkcli(zkOpts);

  var seriesCallback = function(error, result) {
    log.debug('getBrokers/seriesCallback');
    zk.close();
    return cb(error, result);
  }

  async.waterfall([
      function connectZookeeper(callback) {
        log.debug('getBrokers/connectZookeeper');
        zk.connect(function(error) { return callback(error); });
      },
      function getBrokerList(callback) {
        log.debug('getBrokers/getBrokerList');
        zk.a_get_children(brokerIdPath, false, function(rc, error, children) {
          if (rc != 0) return callback(error);
          if (children.length == 0) return callback('No brokers');
          callback(null, children);
        });
      },
      function getBrokers(brokerList, callback) {
        log.debug('getBrokers/getBrokers');
        var result = [];
        async.each(brokerList, function(brokerId, brokerCallback) {
          var brokerPath = brokerIdPath + '/' + brokerId;
          zk.a_get(brokerPath, false, function(rc, error, stat, data) {
            if (rc != 0) return brokerCallback(error);
            var brokerKeys = ['id', 'name', 'host', 'port'];
            var brokerData = _.union(brokerId, data.split(':'));
            var broker = _.object(brokerKeys, brokerData);
            result.push(broker);
            brokerCallback();
          });
        }, function(error) { callback(error, result); });
      }
  ], seriesCallback);
};

Zookeeper.prototype.getConsumerOffsets = function(topic, group, cb) {
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  log.debug('getConsumerOffsets');
  var that = this, groupTopicPath = '/consumers/' + group + '/offsets/' + topic;
  var zk = new zkcli(zkOpts);

  var seriesCallback = function(error, result) {
    log.debug('getConsumerOffsets/seriesCallback');
    zk.close();
    return cb(error, result);
  };

  async.waterfall([
      function connectZookeeper(callback) {
        log.debug('getConsumerOffsets/connectZookeeper');
        zk.connect(function(error) { return callback(error); });
      },
      function createGroupTopicPath(callback) {
        log.debug('getConsumerOffsets/createGroupTopicPath');
        zk.mkdirp(groupTopicPath, function(error) { return callback(error); });
      },
      function getBrokerPartitions(callback) {
        log.debug('getConsumerOffsets/getBrokerPartitions');
        zk.a_get_children(groupTopicPath, false, function(rc, error, brokerPartitions) {
          if (rc != 0) return callback(error);
          return callback(null, brokerPartitions);
        });
      },
      function initializePartitions(brokerPartitions, callback) {
        log.debug('getConsumerOffsets/initializePartitions');
        if (brokerPartitions.length == 0) {
          that.initializeConsumerOffsets(topic, group, function(error) {
            if (error) return callback(error);
            return setImmediate(that.getConsumerOffsets(topic, group, cb));
          });
        } else {
          callback(null, brokerPartitions);
        }
      },
      function getOffsets(brokerPartitions, callback) {
        log.debug('getConsumerOffsets/getOffsets');
        var result = [];
        async.each(brokerPartitions, function(brokerPartitionLabel, brokerPartitionCb) {
          var brokerPartitionPath = groupTopicPath + '/' + brokerPartitionLabel;
          zk.a_get(brokerPartitionPath, false, function(rc, error, stat, data) {
            if (rc != 0) return brokerPartitionCb(error);
            var brokerPartition = _.object(['broker', 'partition'],
              brokerPartitionLabel.split('-'));

            result.push({
                broker: brokerPartition['broker'],
                partition: brokerPartition['partition'],
                offset: data
            });

            brokerPartitionCb();
          });
        }, function(error) { return callback(error, result); });
      }
  ], seriesCallback);
};

Zookeeper.prototype.setConsumerOffsets = function(topic, group, offsets, cb) {
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  log.debug('setConsumerOffsets');
  if (!offsets || _.isEmpty(offsets)) return cb('No offsets provided');
  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;
  var zk = new zkcli(zkOpts);

  var seriesCallback = function(error) {
    cb(error, zk);
    zk.close();
  };

  async.series({
      connectZookeeper: function(callback) { zk.connect(callback); },
      createGroupTopicPath: function(callback) { zk.mkdirp(groupTopicPath, callback); },
      processOffsets: function(callback) {
        async.each(offsets, function(offset, offsetCallback) {
          var offsetPath = groupTopicPath + '/' + offset.broker + '-' + offset.partition;

          zk.a_exists(offsetPath, false, function(rc, error, stat) {
            if (rc != 0) {
              if (error == 'no node') {
                return zk.a_create(offsetPath, offset, null, function(rc, error) {
                  return offsetCallback((rc != 0) ? error : null);
                });
              } else {
                return offsetCallback('Error retriving offset: ' + error);
              }
            } else {
              zk.a_set(offsetPath, offset.offset, stat.version, function(rc, error) {
                return offsetCallback((rc != 0) ? error : null);
              });
            }
          });
        }, callback);
      }
  }, seriesCallback);
};

Zookeeper.prototype.initializeConsumerOffset = function(consumer, group, cb) {
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  var that = this;

  var onGetLatestOffset = function(error, offset) {
    if (error) return cb('Error initializing offset: ' + error);
    var newOffset = [{
        broker: consumer.id,
        partition: consumer.partition,
        offset: offset.toString()
    }];

    that.setConsumerOffsets(consumer.topic, group, newOffset, onSetConsumerOffsets);
  };

  var onSetConsumerOffsets = function(error) {
    if (error) return cb('Error initializing consumer offset: ' + error);
    cb(null, consumer);
  };

  consumer.getLatestOffset(onGetLatestOffset);
};

// Initialize consumer offsets for each broker/partition,
// and return them in the callback like getConsumerOffsets would
// [{ broker: 0, partition: 0, offset: 0 }, ...]
Zookeeper.prototype.initializeConsumerOffsets = function(topic, group, cb) {
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  var that = this;
  var zk = new zkcli(zkOpts);
  var groupTopicPath = '/consumers/' + group + '/offsets/' + topic;

  var seriesCallback = function(error, result) {
    zk.close();
    cb(error, result);
  };

  async.waterfall([
      function connectZookeeper(callback) {
        log.debug('initializeConsumerOffsets/connectZookeeper');
        zk.connect(function(error) { callback(error); });
      },
      function createGroupTopicPath(callback) {
        zk.mkdirp(groupTopicPath, function(error) { callback(error); });
      },
      function getBrokers(callback) {
        log.debug('initializeConsumerOffsets/getBrokers');
        that.getBrokers(function(error, result) { callback(error, result); });
      },
      function getPartitions(brokers, callback) {
          log.debug('initializeConsumerOffsets/getPartitions');
          // After broker list is retrieved, iterate and initialize partition offsets
          var partitionResult = [];
          async.each(brokers, function(broker, brokerCb) {
            that.getPartitions(topic, broker, function(error, partitions) {
              if (error) return brokerCb(error);
              partitionResult = _.union(partitionResult, partitions);
              brokerCb();
            });
          }, function(error) { callback(error, partitionResult); });
      },
      function initializePartitions(partitions, callback) {
        log.debug('initializeConsumerOffsets/initializePartitions');
        var result = [];

        async.each(partitions, function(partition, partitionCb) {
          var offsetPath = groupTopicPath + '/' + partition.broker + '-' + partition.index;

          log.info('Initializing offset to 0: %s', offsetPath);
          zk.a_exists(offsetPath, false, function(rc, error, stat) {
            if (rc != 0) {
              if (error == 'no node') {
                log.debug('initializeConsumerOffsets/initializePartitions/noOffsets');
                zk.a_create(offsetPath, '0', null, function(rc, error) {
                  if (rc != 0) {
                    partitionCb(error);
                  } else {
                    result.push({ broker: partitions.broker, partition: partition.index, offset: '0' });
                    partitionCb();
                  }
                });
              } else {
                log.debug('initializeConsumerOffsets/initializePartitions/unknownError');
                partitionCb(error);
              }
            } else {
              log.debug('initializeConsumerOffsets/initializePartitions/setOffsets');
              zk.a_set(offsetPath, 0, stat.version, function(rc, error) {
                if (rc != 0) {
                  partitionCb(error);
                } else {
                  result.push({ broker: partitions.broker, partition: partition.index, offset: '0' });
                  partitionCb();
                }
              });
            }
          });
        }, function(error) {
          callback(error, result);
        });
      }
  ], seriesCallback);
};

Zookeeper.prototype.getPartitions = function(topic, broker, cb) {
  if (!cb || !_.isFunction(cb)) return console.trace('Callback error');
  log.debug('getPartitions');
  var topicBrokerPath = '/brokers/topics/' + topic + '/' + broker.id;
  var zk = new zkcli(zkOpts);

  var seriesCallback = function(error, result) {
    log.debug('getPartitions/seriesCallback/result', result);
    zk.close();
    cb(error, result);
  };

  async.waterfall([
      function connectZookeeper(callback) {
        zk.connect(function(error) { callback(error); });
      },
      function getPartitions(callback) {
        zk.a_get(topicBrokerPath, false, function(rc, error, stat, data) {
          if (rc != 0 ) return callback('Error retrieving topic paritions: ' + error);
          var partitionCount = Number(data);
          var result = _.range(partitionCount);

          result.forEach(function(partition, index) {
            result[index] = {
              index: index,
              broker: broker.id
            };
          });

          callback(null, result);
        });
      }
  ], seriesCallback);
};

module.exports = Zookeeper;

