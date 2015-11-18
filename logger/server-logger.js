/* globals process */

var _ = require('underscore');
var Promise = require('bluebird');
var events = require('events');
var util = require('util');
var common = require('./common');
var console_override = require('./console-override');
var read_config = require('./logger-config-reader');

var winston = require('winston');
require('winston-syslog').Syslog;
require('./winston-rotate').Rotate;

var LEVELS = common.LEVELS;

var APP_CATEGORY = common.APP_CATEGORY;

var DEFAULT_TRANSPORTS = {
    console: {
        json: false,
        label: '',
        timestamp: true,
        prettyPrint: false,
        colorize: false
    }
};

var app_container = new winston.Container();

// winston app logger instance shared across all Jut loggers
var app_logger = init_logger(APP_CATEGORY, DEFAULT_TRANSPORTS);

var exports = module.exports;

function init_logger(category, config) {
    // gracefully close and remove current loggers
    app_container.close();

    var logger = app_container.get(category, config);
    var transports = logger.transports;

    // why check for silly? winston adds a console transport
    // with a silly level if one is not provided :/
    var silly = _.find(transports, function (transport) {
        return transport.name === 'console'
            && transport.level === 'silly';
    });

    if (silly) {
        delete transports.console;
        logger._names = _.without(logger._names, 'console');
    }

    return logger;
}

function error_arg(err) {
    if (err.stack) {
        return err.stack;
    } else if (err.message) {
        return 'Error: ' + err.message;
    } else {
        return 'unknown error';
    }
}

exports.DEFAULT_TRANSPORTS = DEFAULT_TRANSPORTS;

function ServerLogger(name, level) {
    events.EventEmitter.call(this);

    this.name = name;
    this.level = level;

    // .{level} function for each supported level
    this._bindLevels();
}

util.inherits(ServerLogger, events.EventEmitter);

ServerLogger.prototype._bindLevels = function () {
    _.each(LEVELS, this._bindLevel, this);
};

ServerLogger.prototype._bindLevel = function (method, index) {
    var self = this;

    function buildArgs(method, args, callback) {
        // winston has a bug wrt digesting exceptions, so we do it here
        _.each(args, function (arg, index) {
            if (arg instanceof Error) {
                args[index] = error_arg(arg);
            }
        });

        args.unshift('[' + self.name + '] (' + process.pid + ')');
        args.unshift(method);

        // use winston's log callback to emit logged events
        args.push(function (err, level, msg, meta) {
            self.emit('logged', err, level, msg, meta);
            if (callback) { callback(); }
        });

        return args;
    }

    this[method] = function () {
        if (self.level < index) {
            // noop
            return;
        }
        var args = buildArgs(method, Array.prototype.slice.call(arguments));
        app_logger.log.apply(app_logger, args);
    };

    this[method + 'Async'] = function() {
        var grab_args = Array.prototype.slice.call(arguments);
        return new Promise(function(resolve, reject) {
            if (self.level < index) {
                // noop
                return;
            }
            var args = buildArgs(method, grab_args, resolve);
            app_logger.log.apply(app_logger, args);
        });
    };
};

ServerLogger.prototype.setLevel = function(level) {
    this.level = common.to_level_code(level);
};

exports.transports = function (transports) {
    set_transports(transports);
    set_transport_threshold();
};

function set_transports(transports) {
    transports = transports || {};

    // new transports provided
    if (!_.isEmpty(transports)) {
        app_logger = init_logger(APP_CATEGORY, transports);
    }
}

function set_transport_threshold() {
    // use debug threshold for transports.
    // The real level is maintained by ServerLogger
    var threshold = 'debug';

    var loggers = app_container.loggers;
    _.each(_.values(loggers), function (logger) {
        logger.level = threshold;
        _.each(_.values(logger.transports), function (transport) {
            if (transport.level) {
                transport.level = threshold;
            }
        });
    });
}

function query(options, callback) {
    app_logger.query(options, callback);
}

exports.Logger = ServerLogger;

function production_console_override() {
    /* global process */
    if (process.env.NODE_ENV === 'production') {
        console_override();
    }
}

// This is called once when the module is loaded, after the base module has been
// initialized.
exports.init_module = function(logger) {
    logger.config(read_config());
    production_console_override();

    logger.query = query;
};
