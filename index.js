var dir = './lib/';

if (process.env.KAFKAZK_COVERAGE){
  var dir = './lib-cov/';
}

exports.Zookeeper = require(dir + 'Zookeeper');
