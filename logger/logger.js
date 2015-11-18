var _ = require('underscore');
var common = require('./common');
var to_level_code = common.to_level_code;

var IS_SERVER = true; //Logic remains from when we were using require_utils

var browser = require('./browser-logger');
var server = require('./server-logger');

var ALL_LEVELS = common.ALL_LEVELS;
var DEFAULT_LEVEL = common.DEFAULT_LEVEL;

var CONFIG = {};
var LOGGERS = {};
var UNIQUE = {};

var exports = module.exports;

function get(name) {
    var clazz = IS_SERVER ? server.Logger : browser.Logger;
    return new_logger(name, clazz);
}
exports.get = get;

exports.delete = function(name) {
    delete LOGGERS[name];
};

exports.get_unique = function (name) {
    return get(next_unique(name));
};

function next_unique(name) {
    var id = UNIQUE[name] || 0;
    UNIQUE[name] = id + 1;

    return [name, '[', id, ']'].join('');
}

function new_logger(name, clazz) {
    var level = level_for_logger(name);
    var logger = new clazz(name, level);

    LOGGERS[name] = logger;
    return logger;
}

exports.reset = function () {
    var defaults = build_default_config();
    config(defaults);
};

function build_default_config() {
    var defaults = {};

    var levels = {};
    levels[ALL_LEVELS] = DEFAULT_LEVEL;
    defaults.levels = levels;

    if (IS_SERVER) {
        defaults.transports = server.TRANSPORTS;
    }
    return defaults;
}

function config(conf) {
    CONFIG = conf || {};

    if (IS_SERVER) {
        server.transports(CONFIG.transports);
    }

    set_levels();
}
exports.config = config;

function get_config() {
    return CONFIG;
}
exports.get_config = get_config;

function set_levels() {
    var levels = CONFIG.levels || {};
    _.each(_.keys(levels), function (name) {
        set_level(name, levels[name]);
    });
}

function set_level(name, level) {
    // set global level for all transports
    if (name === ALL_LEVELS) {
        set_logger_levels(level);
    } else {
        set_logger_level(name, level);
    }
}

exports.set_level = set_level;

function set_logger_levels(level) {
    _.each(_.values(LOGGERS), function (logger) {
        logger.level = to_level_code(level);
    });
}

function set_logger_level(name, level) {
    // set specific logger levels
    var logger = _.find(_.values(LOGGERS), function (logger) {
        return logger.name === name;
    });

    if (logger) {
        logger.level = to_level_code(level);
    }
}

function level_for_logger(name) {
    var level_code = get_default_code();
    var levels = CONFIG.levels || {};

    _.each(levels, function (level, target) {
        if (name.slice(0, target.length) === target) {
            level_code = Math.max(level_code, to_level_code(level));
        }
    });

    return level_code;
}

function get_default_level() {
    var levels = CONFIG.levels || {};
    return levels[ALL_LEVELS] || DEFAULT_LEVEL;
}
exports.get_default_level = get_default_level;

function get_default_code() {
    var default_level = get_default_level();
    var default_code = to_level_code(default_level);

    return default_code;
}
exports.get_default_code = get_default_code;

exports.default_config = function (file) {
    var transports = {
        rotate: {
            file: file,
            colorize: false,
            timestamp: true,
            json: false,
            max: '100m',
            keep: 5,
            compress: true
        }
    };

    var default_config = build_default_config();
    default_config.transports = transports;

    return default_config;
};

// logger configuration to use when running in dev mode
exports.dev_config = function () {
    var transports = {
        console: {
            json: false,
            label: '',
            timestamp: true,
            prettyPrint: false,
            colorize: true
        }
    };

    var dev_config = build_default_config();
    dev_config.transports = transports;
    dev_config.levels = CONFIG.levels;

    return dev_config;
};

if (IS_SERVER) {
    server.init_module(exports);
} else {
    browser.init_module(exports);
}
