var _ = require('underscore');
_.str = require('underscore.string');
_.mixin(_.str.exports());

var async = require('async');
var bignum = require('bignum');
var zlib = require('zlib');
var Zookeeper = require('zookeeper');
var Consumer = require('prozess').Consumer;
var path = require('path');

/**
 * Provide dummy logger if none provided
 */
var noop = function () {
};
var Logger = function () {
    this.debug = noop;
    this.info = noop;
    this.warn = noop;
};

/**
 * Construct object for connecting to kafka via zookeeper.
 *
 * Where possible tasks are performed asynchronous and/or in parallel.
 *
 * Example of usage:
 *    var log = new Logger(); // user your favorite logger here
 *    var kafka = new Kafkazoo();
 *
 *    var onMessages = function(error, messages, acknowledge, newOffset) {
 *      if (error) return log.error(error);
 *      log.info('Received %d messages', messages.length);
 *      log.debug(messages[0].substring(0, 100) + '...');
 *      // newOffset information is provided in case you want to use it
 *      acknowledge(true); // false wil resend the messages with by default 5 sec delay
 *    };
 *
 *    kafka.consume('topicname', 'consumergroupname', onMessages);
 *
 *
 * @param options
 *  host: zookeeper host, defaults to localhost
 *  port: zookeeper port, defaults to 2181
 *  zkPath: zookeeper path, defaults to '/'
 *  noMessageTimeout: delay in ms if no messages are received, defaults to 30000
 *  failedMessageTimeout: delay in ms to resend batch if not acknowledged, defaults to 5000
 *  logger: logger to use (using debug, info, warn), defaults to dummy
 *
 * @constructor
 *
 * TODO: allow multiple zookeeper servers / provide zkConfig directly
 * TODO: add graceful consumption ending
 */
var Kafkazoo = function (options) {
    this.config = options || {};
    this.zkConfig = {
        connect: (this.config.host || 'localhost') + ':' + (this.config.port || 2181),
        timeout: this.config.timeout || 30000,
        debug_level: Zookeeper.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false,
        data_as_buffer: false
    };
    this.config.zkPath = this.config.zkPath || '/';
    this.config.noMessageTimeout = this.config.noMessageTimeout || 10000;
    this.config.failedMessageTimeout = this.config.failedMessageTimeout || 5000;
    this.log = this.config.logger || new Logger();
};

/**
 * Consume messages and provides it to the callback. Main method to use.
 *
 * Connects all consumers and starts them. When messages are received, they are
 * decompressed (if needed).
 * By acknowledging the messages, the offset is adjusted. Otherwise the messages will
 * be resend with the configured dalay.
 *
 * @param topic: topic to subscribe to
 * @param consumerGroup: consumer group name
 * @param onMessages: callback that processes the received batched messages
 */
Kafkazoo.prototype.consume = function (topic, consumerGroup, onMessages) {
    this.log.debug('consumeTopic');
    if (!onMessages || !_.isFunction(onMessages)) return console.trace('onMessages error');

    var that = this;
    var consumers;

    async.waterfall([
        function getConsumers(asyncReady) {
            that.getConsumers(topic, consumerGroup, function (error, result) {
                asyncReady(error, result);
            });
        },
        function connectConsumers(consumers, asyncReady) {
            that.log.debug('consume/connectConsumers');
            async.each(consumers, function (consumer, asyncConsumerReady) {
                that.log.info('Connecting consumer %s-%s...', consumer.host, consumer.partition);
                consumer.connect(function (error) {
                    asyncConsumerReady(error);
                });
            }, function consumerReady(error) {
                asyncReady(error, consumers);
            });
        },
        function startConsuming(consumers, asyncReady) {
            that.log.debug('consume/startConsuming');
            async.each(consumers, function (consumer, asyncConsumerReady) {
                onConsumerReady(consumer);
                asyncConsumerReady();
            });
            asyncReady();
        }
    ], function ready(error) {
        that.log.debug('consume/ready');
        if (error) onMessages(error);
    });

    var onConsumerReady = function (consumer) {
        that.log.debug('Consumer for %s-%s ready', consumer.host, consumer.partition);
        consumer.consume(function (error, messages) {
            onConsume(error, messages, consumer);
        });
    }

    var onConsume = function (error, messages, consumer) {
        that.log.debug('consume/onConsume');
        if (error) {
            if (error.message == 'OffsetOutOfRange') {
                that.log.info('Offset out of range; re-initializing');
                that.initializeConsumerOffset(consumer, consumerGroup, function (error) {
                    if (error) return onMessages('Error retrieving consumer offset: ' + error);
                    onConsumerReady(consumer);
                });
                return;
            } else {
                return onMessages('Unhandled error while consuming: ' + error);
            }
        }

        that.decompressMessages(messages, function (error, result) {
            if (messages.length == 0) {
                that.log.debug('No messages');
                return setTimeout(function () {
                    onConsumerReady(consumer)
                }, that.config.noMessageTimeout);
            }

            var newOffset = {
                broker: consumer.id,
                partition: consumer.partition,
                offset: bignum(consumer.offset).toString()
            };

            var onAck = function (ack) {
                if (ack) {
                    that.setConsumerOffsets(topic, consumerGroup, [newOffset], function (error) {
                        if (error) return console.trace(error);
                        onConsumerReady(consumer);
                    });
                } else {
                    that.log.warn('Messages failed by client; retrying with delay...');
                    setTimeout(function () {
                        onConsume(null, messages);
                    }, that.config.failedMessageTimeout);
                }
            };
            onMessages(error, result, onAck, newOffset);
        });
    }
};

/**
 * For backward compatibility
 */
Kafkazoo.prototype.consumeTopic = this.consume;

/**
 * Decompress messages.
 *
 * @param messages
 * @param onReady
 *
 * TODO: implement Snappy compression
 */
Kafkazoo.prototype.decompressMessages = function (messages, onReady) {
    var result = [];

    async.each(messages, function (message, asyncMessageReady) {
        switch (message.compression) {
            case 0:
                result.push(message.payload);
                asyncMessageReady();
                break;
            case 1:
                zlib.gunzip(message.payload, function (error, buffer) {
                    if (error) return asyncMessageReady(error);

                    while (buffer.length > 0) {
                        var size = buffer.readUInt32BE(0) + 4;
                        var msg = buffer.toString('utf8', 10, size);
                        result.push(msg);
                        buffer = buffer.slice(size);
                    }

                    return asyncMessageReady();
                });
                break;
            case 2:
                asyncMessageReady('Snappy not implemented');
                break;
            default:
                asyncMessageReady('Unknown compression: ' + message.compression);
                break;
        }
    }, function ready(error) {
        onReady(error, result);
    });
};

/**
 * Get all consumers within a consumer group for a topic.
 *
 * Retrieve all brokers and relevant consumer offsets.
 * Create zero offsets for each broker/partition if none exist.
 * Define consumer options (broker meta data)
 * Create the consumers
 *
 * @param topic
 * @param consumerGroup
 * @param onReady
 */
Kafkazoo.prototype.getConsumers = function (topic, consumerGroup, onReady) {
    this.log.debug('getConsumers');
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');

    var that = this;

    async.waterfall([
        function getTopicGroupData(asyncReady) {
            that.log.debug('getConsumers/getTopicGroupData');
            async.parallel({
                getBrokers: function (asyncTopicDataReady) {
                    that.getBrokers(function (error, result) {
                        asyncTopicDataReady(error, result);
                    });
                },
                getConsumerOffsets: function (nestedasyncReady) {
                    that.getConsumerOffsets(topic, consumerGroup, function (error, result) {
                        nestedasyncReady(error, result);
                    });
                }
            }, function topicDataReady(error, results) {
                asyncReady(error, results.getBrokers, results.getConsumerOffsets);
            });
        },
        function initializeOffsetsConditional(brokers, offsets, asyncReady) {
            if (_.isEmpty(offsets)) {
                that.log.debug('getConsumers/initializeOffsetsConditional/initializeConsumerOffsets');
                that.initializeConsumerOffsets(topic, consumerGroup, function (error, result) {
                    if (error) return asyncReady(error);
                    asyncReady(null, brokers, result);
                });
            } else {
                asyncReady(null, brokers, offsets);
            }
        },
        function createConsumerOptions(brokers, offsets, asyncReady) {
            var result = [];
            async.each(offsets, function (offset, asyncOffsetReady) {
                var broker = {
                    'broker': _.chain(brokers)
                        .where({ id: offset.broker }) // Pull remaining broker data from
                        .first().value()               // broker collection
                };

                result.push(_.chain(offset)
                    .omit('broker') // Remove broker id
                    .extend(broker) // Add entire broker metadata object
                    .value());

                asyncOffsetReady();
            }, function offsetReady(error) {
                asyncReady(error, result);
            });
        },
        function createConsumers(optionsArray, asyncReady) {
            var result = [];
            async.each(optionsArray, function (options, asyncConsumerReady) {
                var consumer = new Consumer({
                    host: options.broker.host,
                    port: options.broker.port,
                    topic: topic,
                    partition: options.partition,
                    offset: new bignum(options.offset)
                });
                consumer.id = options.broker.id;
                result.push(consumer);
                asyncConsumerReady();
            }, function consumersReady(error) {
                return asyncReady(error, result);
            });
        }
    ], function ready(error, result) {
        that.log.debug('getConsumers/seriesCallback');
        onReady(error, result);
    });
};

/**
 * Get all brokers.
 *
 * Interacts with zookeeper
 *
 * @param onReady
 * @returns {*}
 */
Kafkazoo.prototype.getBrokers = function (onReady) {
    this.log.debug('getBrokers');
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');

    var that = this;

    this.interactWithZookeeper(
        '/brokers/ids',
        function onConnected(zookeeper, datapath, asyncReady) {
            async.waterfall([
                function getBrokerList(nestedAsyncReady) {
                    that.log.debug('getBrokers/getBrokerList');
                    zookeeper.a_get_children(datapath, false, function (resultCode, error, children) {
                        if (resultCode != 0) return nestedAsyncReady(error);
                        if (children.length == 0) return nestedAsyncReady('No brokers');
                        nestedAsyncReady(null, children);
                    });
                },
                function getBrokers(brokerList, nestedAsyncReady) {
                    that.log.debug('getBrokers/getBrokers');
                    var result = [];
                    async.each(brokerList, function (brokerId, asyncBrokerReady) {
                        var brokerPath = datapath + '/' + brokerId;
                        zookeeper.a_get(brokerPath, false, function (resultCode, error, stat, data) {
                            if (resultCode != 0) return asyncBrokerReady(error);
                            var brokerKeys = ['id', 'name', 'host', 'port'];
                            var brokerData = _.union(brokerId, data.split(':'));
                            var broker = _.object(brokerKeys, brokerData);
                            result.push(broker);
                            asyncBrokerReady();
                        });
                    }, function brokersReady(error) {
                        nestedAsyncReady(error, result);
                    });
                }
            ], function ready(error, result) {
                that.log.debug('getBrokers/ready');
                return asyncReady(error, result);
            });
        },
        function ready(error, result) {
            onReady(error, result);
        });

};


/**
 * Get consumer offsets
 *
 * Interacts with zookeeper.
 * - Get all broker partitions
 * - Initialize partions (if needed)
 * - Get offsets
 *
 * @param topic
 * @param consumerGroup
 * @param onReady
 * @returns {*}
 */
Kafkazoo.prototype.getConsumerOffsets = function (topic, consumerGroup, onReady) {
    this.log.debug('getConsumerOffsets');
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');

    var that = this

    this.interactWithZookeeper(
        '/consumers/' + consumerGroup + '/offsets/' + topic,
        function onConnected(zookeeper, datapath, asyncReady) {
            async.waterfall([
                function getBrokerPartitions(nestedAsyncReady) {
                    that.log.debug('getConsumerOffsets/getBrokerPartitions');
                    zookeeper.a_get_children(datapath, false, function (resultCode, error, brokerPartitions) {
                        if (resultCode != 0) return nestedAsyncReady(error);
                        return nestedAsyncReady(null, brokerPartitions);
                    });
                },
                function initializePartitions(brokerPartitions, nestedAsyncReady) {
                    that.log.debug('getConsumerOffsets/initializePartitions');
                    if (brokerPartitions.length == 0) {
                        that.initializeConsumerOffsets(topic, consumerGroup, function (error) {
                            if (error) return nestedAsyncReady(error);
                            return setImmediate(that.getConsumerOffsets(topic, consumerGroup, onReady));
                        });
                    } else {
                        nestedAsyncReady(null, brokerPartitions);
                    }
                },
                function getOffsets(brokerPartitions, nestedAsyncReady) {
                    that.log.debug('getConsumerOffsets/getOffsets');
                    var result = [];
                    async.each(brokerPartitions, function (brokerPartitionLabel, asyncBrokerPartitionReady) {
                        var brokerPartitionPath = datapath + '/' + brokerPartitionLabel;
                        zookeeper.a_get(brokerPartitionPath, false, function (resultCode, error, stat, data) {
                            if (resultCode != 0) return asyncBrokerPartitionReady(error);
                            var brokerPartition = _.object(['broker', 'partition'],
                                brokerPartitionLabel.split('-'));

                            result.push({
                                broker: brokerPartition['broker'],
                                partition: brokerPartition['partition'],
                                offset: data
                            });

                            asyncBrokerPartitionReady();
                        });
                    }, function brokerPartitionReady(error) {
                        return nestedAsyncReady(error, result);
                    });
                }
            ], function ready(error, result) {
                that.log.debug('getConsumerOffsets/seriesCallback');
                return asyncReady(error, result);
            });
        },
        function ready(error, result) {
            onReady(error, result);
        });

};
/**
 *
 * @param topic
 * @param group
 * @param offsets
 * @param onReady
 */
Kafkazoo.prototype.setConsumerOffsets = function (topic, group, offsets, onReady) {
    this.log.debug('setConsumerOffsets');
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');
    if (!offsets || _.isEmpty(offsets)) return onReady('No offsets provided');

    this.interactWithZookeeper(
        '/consumers/' + group + '/offsets/' + topic,
        function onConnected(zookeeper, datapath, asyncReady) {
            async.each(offsets, function (offset, asyncOffsetReady) {
                var offsetPath = datapath + '/' + offset.broker + '-' + offset.partition;
                zookeeper.a_exists(offsetPath, false, function (resultCode, error, stat) {
                    if (resultCode != 0) {
                        if (error == 'no node') {
                            return zookeeper.a_create(offsetPath, offset, null, function (resultCode, error) {
                                return asyncOffsetReady((resultCode != 0) ? error : null);
                            });
                        } else {
                            return asyncOffsetReady('Error retrieving offset: ' + error);
                        }
                    } else {
                        zookeeper.a_set(offsetPath, offset.offset, stat.version, function (resultCode, error) {
                            return asyncOffsetReady((resultCode != 0) ? error : null);
                        });
                    }
                });
            }, function ready(error) {
                asyncReady(error)
            });
        },
        function ready(error, result) {
            onReady(error, result);
        });

};

/**
 *
 * @param consumer
 * @param group
 * @param onReady
 */
Kafkazoo.prototype.initializeConsumerOffset = function (consumer, group, onReady) {
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');
    var that = this;

    var onSetConsumerOffsets = function (error) {
        if (error) return onReady('Error initializing consumer offset: ' + error);
        onReady(null, consumer);
    };

    var onGetLatestOffset = function (error, offset) {
        if (error) return onReady('Error initializing offset: ' + error);
        var newOffset = [
            {
                broker: consumer.id,
                partition: consumer.partition,
                offset: offset.toString()
            }
        ];

        that.setConsumerOffsets(consumer.topic, group, newOffset, onSetConsumerOffsets);
    };


    consumer.getLatestOffset(onGetLatestOffset);
};


/**
 * Orchestrate all interactions with zookeeper.
 *
 * Connects to zookeeper for the duration of the call (closes the connection afterwards).
 * Creates the requested path (if not existing), and adjusts for a root path.
 *
 * @param relativePath: path in zookeeper to use (without root path)
 * @param onConnected: callback with actions to perform after connecting with zookeeper
 * @param onReady: callback when done
 */
Kafkazoo.prototype.interactWithZookeeper = function (relativePath, onConnected, onReady) {
    this.log.info('interactWithZookeeper');
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');
    if (!onConnected || !_.isFunction(onConnected)) return console.trace('onConnected error');

    var that = this;
    var zookeeper = new Zookeeper(this.zkConfig);
    var datapath = path.join(this.config.zkPath, relativePath);

    async.waterfall([
        function connect(asyncReady) {
            that.log.debug('interactWithZookeeper/connect');
            zookeeper.connect(function (error) {
                asyncReady(error);
            });
        },
        function createPath(asyncReady) {
            that.log.debug('interactWithZookeeper/createPath');
            zookeeper.mkdirp(datapath, function (error) {
                asyncReady(error);
            });
        },
        function nextActions(asyncReady) {
            that.log.debug('interactWithZookeeper/nextActions');
            onConnected(zookeeper, datapath, asyncReady);
        }
    ], function (error, result) {
        that.log.debug('interactWithZookeeper/ready');
        zookeeper.close();
        onReady(error, result);
    });

};


/**
 * Initialize consumer offsets for each partition on each broker.
 *
 * Interacts with zookeeper:
 *  - get all partitions from all brokers
 *  - initialize these partitions
 * Returns consumer offsets in the callback like getConsumerOffsets would
 *
 *      [{ broker: 0, partition: 0, offset: 0 }, ...]
 *
 * @param topic
 * @param consumerGroup
 * @param onReady
 */
Kafkazoo.prototype.initializeConsumerOffsets = function (topic, consumerGroup, onReady) {
    this.log.debug('initializeConsumerOffsets');
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');

    var that = this;

    this.interactWithZookeeper(
        '/consumers/' + consumerGroup + '/offsets/' + topic,
        function onConnected(zookeeper, datapath, onReady) {
            async.waterfall([
                function getBrokers(asyncReady) {
                    that.log.debug('initializeConsumerOffsets/getBrokers');
                    that.getBrokers(function (error, result) {
                        asyncReady(error, result);
                    });
                },
                function getPartitions(brokers, asyncReady) {
                    that.log.debug('initializeConsumerOffsets/getPartitions');
                    var allPartitions = [];
                    async.each(brokers, function getBrokerPartions(broker, asyncBrokerReady) {
                        that.getPartitions(topic, broker, function (error, brokerPartitions) {
                            if (error) return asyncBrokerReady(error);
                            allPartitions = _.union(allPartitions, brokerPartitions);
                            asyncBrokerReady();
                        });
                    }, function ready(error) {
                        asyncReady(error, allPartitions);
                    });
                },
                function initializePartitions(partitions, asyncReady) {
                    that.log.debug('initializeConsumerOffsets/initializePartitions');
                    var result = [];

                    async.each(partitions, function (partition, asyncPartitionReady) {
                        var offsetPath = datapath + '/' + partition.broker + '-' + partition.index;
                        var onOffsetReady = function (resultCode, error) {
                            if (resultCode != 0) {
                                asyncPartitionReady(error);
                            } else {
                                result.push({ broker: partitions.broker, partition: partition.index, offset: '0' });
                                asyncPartitionReady();
                            }
                        };

                        that.log.info('Initializing offset to 0: %s', offsetPath);
                        zookeeper.a_exists(offsetPath, false, function (resultCode, error, stat) {
                            if (resultCode != 0) {
                                if (error == 'no node') {
                                    that.log.debug('initializeConsumerOffsets/initializePartitions/noOffsets');
                                    zookeeper.a_create(offsetPath, '0', null, onOffsetReady);
                                } else {
                                    that.log.debug('initializeConsumerOffsets/initializePartitions/unknownError');
                                    asyncPartitionReady(error);
                                }
                            } else {
                                that.log.debug('initializeConsumerOffsets/initializePartitions/setOffsets');
                                zookeeper.a_set(offsetPath, 0, stat.version, onOffsetReady);
                            }
                        });
                    }, function partitionsReady(error) {
                        asyncReady(error, result);
                    });
                }], function ready(error, result) {
                onReady(error, result);
            });
        });
};


/**
 * Get all the partitions for a topic from a broker.
 *
 * Interacts with zookeeper.
 *
 * @param topic
 * @param broker
 * @param onReady
 *
 * TODO: reuse existing zookeeper connection?
 */
Kafkazoo.prototype.getPartitions = function (topic, broker, onReady) {
    this.log.debug('getPartitions');
    if (!onReady || !_.isFunction(onReady)) return console.trace('onReady error');

    this.interactWithZookeeper(
        '/brokers/topics/' + topic + '/' + broker.id,
        function onConnected(zookeeper, datapath, asyncReady) {
            zookeeper.a_get(datapath, false, function (resultCode, error, stat, data) {
                if (resultCode != 0) return callback('Error retrieving topic paritions: ' + error);

                var partitionCount = Number(data);
                var result = _.range(partitionCount);
                result.forEach(function (partition, index) {
                    result[index] = {
                        index: index,
                        broker: broker.id
                    };
                });
                asyncReady(null, result);
            });
        }, function ready(error, result) {
            onReady(error, result);
        }
    );
};


/**
 * Delivery
 */
module.exports = Kafkazoo;

