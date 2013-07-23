var _ = require('underscore');
_.str = require('underscore.string');
_.mixin(_.str.exports());

var should = require('should');
var bignum = require('bignum');
var sinon = require('sinon');
var mockery = require('mockery');

var zkMock = function(options) {
  return {
    connect: function(callback) { callback(); },
    mkdirp: function(path, callback) { callback(); },
    a_get_children: function(path, unused, callback) {
      // callback(return code, error, children)
      if (path == '/brokers/ids') {
        return callback(0, null, [0, 1, 2]);
      } else if (path == '/consumers/testGroup/offsets/testTopic') {
        return callback(0, null, ['0-0', '1-1']);
      } else {
        return callback(1, 'no node', null);
      }
    },
    a_get: function(path, unused, callback) {
      // callback(return code, error, stat, data)
      if (_.startsWith(path, '/brokers/ids/')) {
        return callback(0, null, null, '127.0.0.1-1373962374351:127.0.0.1:9092');
      } else if (_.startsWith(path, '/consumers/testGroup/offsets/testTopic')) {
        return callback(0, null, null, '1000');
      } else {
        return callback(1, 'no node', null, null, null);
      }
    },
    close: function() { }
  };
};

mockery.registerMock('zookeeper', zkMock);
mockery.enable({ warnOnReplace: false, warnOnUnregistered: false });
var kafkaZk = new (require('../index')).Zookeeper();
mockery.disable();

describe('Zookeeper', function() {
  describe('#consumeTopic', function() {
    //kafkaZk.consumeTopic(topic, group, function(error, messages) {
    //});
  });

  describe('#getConsumers', function() {
    it('creates a consumer for each broker and partition', function() {
      //kafkaZk.getConsumers(topic, group, function(error, result) {
      //});
    });
  });

  describe('#getBrokers', function() {
    it('returns an array of kafka brokers stored in zookeeper', function() {
      kafkaZk.getBrokers(function(error, result) {
        should.not.exist(error);
        result.length.should.equal(3);

        _.each(result, function(broker, index) {
          broker.id.should.equal(index);
          broker.name.should.equal('127.0.0.1-1373962374351');
          broker.host.should.equal('127.0.0.1');
          broker.port.should.equal('9092');
        });
      });
    });
  });

  describe('#getConsumerOffsets', function() {
    it('returns an array of kafka consumer offsets', function() {
      kafkaZk.getConsumerOffsets('testTopic', 'testGroup', function(error, result) {
        should.not.exist(error);
        result.length.should.equal(2);

        var zero = _.find(result, function(offset) { return offset.broker == '0'; })
        should.exist(zero);
        zero.partition.should.equal('0');
        zero.offset.should.equal('1000');

        var one = _.find(result, function(offset) { return offset.broker == '1'; })
        should.exist(one);
        one.partition.should.equal('1');
        one.offset.should.equal('1000');
      });
    });

    it('returns an empty array when group is missing', function() {
    });

    it('returns an empty array when topic is missing', function() {
    });
  });

  describe('#setConsumerOffsets', function() {
    it('stores offsets from an array as znodes in zookeeper', function() {
    });
  });

  describe('#initializeConsumerOffset', function() {
    it('sets a znode with the latest offset for a consumer and group', function() {
    });
  });

  describe('#initializeConsumerOffsets', function() {
    it('creates offset nodes if none exist', function() {
    });

    it('resets offset nodes if all exist', function() {
    });
  });

  describe('#getPartitions', function() {
    it('returns an array of partition ids for a topic and broker', function() {
    });

    it('returns an error when topic is missing', function() {
    });

    it('returns an error when broker is missing', function() {
    });
  });
});
