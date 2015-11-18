/* globals console */

var _ = require('underscore');
var common = require('./common');
var logger_config = require('./browser-logger-config');

var exports = module.exports;

var Backbone = require('backbone');

var LEVELS = common.LEVELS;
var DEFAULT_LEVEL = common.DEFAULT_LEVEL;

function BrowserLogger(name, level) {
    this.name = name;
    this.level = level;

    // .{level} function for each supported level
    this._bindLevels();
}

_.extend(BrowserLogger.prototype, Backbone.Events);

BrowserLogger.prototype._bindLevels = function () {
    _.each(LEVELS, this._bindLevel, this);
};

BrowserLogger.prototype._bindLevel = function (method, index) {
    var self = this;

    function buildArgs(method, args) {
        args.unshift('[' + self.name + ']');
        args.unshift(method + ':');
        args.unshift('-');
        args.unshift(new Date().toISOString());

        return args;
    }

    this[method] = function () {
        if (self.level < index) {
            // noop
            return;
        }

        var args = buildArgs(method, Array.prototype.slice.call(arguments, 0));
        var fn = console[method] || console[DEFAULT_LEVEL];
        fn.apply(console, args);

        self.trigger('logged', null, method, args, {});
    };
};

exports.Logger = BrowserLogger;

// Stash a subset of the logger API in the JutLogger global so that it's usable
// from gists and from console
function init_global(logger) {
    function set_and_save_level(name, level) {
        logger.set_level(name, level);
        logger_config.set_level(name, level);
    }

    /* global window */
    window.JutLogger = {
        get: logger.get,
        get_unique: logger.get_unique,
        set_level: set_and_save_level,
        reset: logger_config.reset
    };
}

// This is called once when the module is loaded, after the base module has been
// initialized.
exports.init_module = function(logger) {
    logger.config(logger_config.load());
    init_global(logger);
};
