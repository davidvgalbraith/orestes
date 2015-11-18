var _ = require('underscore');
var exports = module.exports;

var APP_CATEGORY = 'app';
exports.APP_CATEGORY = APP_CATEGORY;

var LEVELS = [
    'error',
    'warn',
    'info',
    'debug'
];
exports.LEVELS = LEVELS;

var ALL_LEVELS = '*';
exports.ALL_LEVELS = ALL_LEVELS;

var DEFAULT_LEVEL = 'info';
exports.DEFAULT_LEVEL = DEFAULT_LEVEL;

exports.to_level_code = function(level) {
    var index = _.indexOf(LEVELS, level);
    if (index === -1) {
        throw new Error('invalid log level ' + level);
    }
    return index;
};
