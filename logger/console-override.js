/* global console */

var Logger = require('./logger');
var util = require('util');

module.exports = function () {
    var logger = Logger.get('console');

    console.log = function () {
        logger.info(util.format.apply(util, arguments));
    };

    console.warn = function () {
        logger.warn(util.format.apply(util, arguments));
    };

    console.error = function () {
        logger.error(util.format.apply(util, arguments));
    };

    console.info = function () {
        logger.info(util.format.apply(util, arguments));
    };

    console.trace = function () {
        logger.debug(util.format.apply(util, arguments));
    };
};
