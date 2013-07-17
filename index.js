var dir = './lib/';

if (process.env.PROZESS_COVERAGE){
  var dir = './lib-cov/';
}

exports.Zookeeper = require(dir + 'Zookeeper');
