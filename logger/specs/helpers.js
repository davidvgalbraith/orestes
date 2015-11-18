var exports = module.exports;
var expect = require('chai').expect;
var Logger = require('../logger');

exports.check_logger = function (logger, level) {
    // info
    level = level || Logger.get_default_code();

    expect(logger).a('object');

    expect(logger.level).to.equal(level);

    expect(logger.error).a('function');
    expect(logger.warn).a('function');
    expect(logger.info).a('function');
    expect(logger.debug).a('function');
};

exports.logger_name = function () {
    var date = new Date();
    var timestamp = date.getTime();

    return ['logger', timestamp].join('-');
};
