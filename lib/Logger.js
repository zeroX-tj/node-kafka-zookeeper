var winston = require('winston');

module.exports = function(loglevel, logfile) {
  this.logger = new (winston.Logger)({
      exitOnError: false,
      transports: [
        new (winston.transports.Console)({
            level: loglevel,
            colorize: true,
            timestamp: true,
            handleExceptions: true
        }),
        new (winston.transports.File)({
            filename: logfile,
            level: loglevel,
            timestamp: true,
            json: false,
            maxsize: 10 * 1024 * 1024 * 1024,
            maxFiles: 2,
            handleExceptions: true
        })
    ]
  });

  this.logger.extend(this);
}

// vim: ft=javascript expandtab shiftwidth=2 softtabstop=2
