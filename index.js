var dir = './lib/';

if (process.env.KAFKAZK_COVERAGE){
  var dir = './lib-cov/';
}

exports.Kafkazoo = require(dir + 'Kafkazoo');
exports.Zookeeper = exports.Kafkazoo; // keep backwards compatible for now