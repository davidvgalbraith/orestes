/* global console,process */

var fs = require('fs');
var path = require('path');
var _ = require('underscore');

// Hack to enable debugging for the config search itself using an
// environment variable.
var debug = process.env.LOGGER_DEBUG ? console.log : function () {
};

/**
 * Read configuration from the file specified by the LOGGER_CONFIG environment variable.
 * If not present, start at the current directory, and walk up the filesystem hierarchy
 * until a file called logger-config.json is found
 */
function read_config_file() {
    var config;

    debug('reading logger config');

    config = try_env_var();
    if (!_.isEmpty(config)) {
        return config;
    }

    config = try_canonical_dir();

    if (_.isEmpty(config)) {
        debug('no config found');
    }
    return config;
}

function try_env_var() {
    debug('trying to read config from LOGGER_CONFIG...');
    var config_file = process.env.LOGGER_CONFIG;
    if (!config_file) {
        return {};
    }

    debug('LOGGER_CONFIG=', config_file);
    return do_read_config(config_file);
}

function try_canonical_dir() {
    debug('trying canonical dir...');

    var dir = path.resolve('.');
    debug('canonical dir', dir);

    var config_file = null;
    while (true) {
        try {
            config_file = path.join(dir, 'logger-config.json');
            return do_read_config(config_file);
        } catch (err) {
            if (err instanceof SyntaxError) {
                console.error('invalid logger config file', config_file);
            }

            if (err.code !== 'ENOENT') {
                throw(err);
            }
        }

        dir = path.dirname(dir);
        if (dir === '/') {
            return {};
        }
    }
}

function do_read_config(config_file) {
    debug('loading', config_file);
    var contents = fs.readFileSync(config_file);
    debug('got', contents.toString());
    var config = JSON.parse(contents);
    debug('parsed config', config);
    return config;
}

// Parse the DEBUG environment variable into the corresponding config
// structure.
function parse_debug_env() {
    var config = {};
    if (typeof process !== 'undefined' &&
        typeof process.env !== 'undefined' &&
        process.env.DEBUG) {
        config.levels = {};
        var targets = process.env.DEBUG.split(',');
        targets.forEach(function (target) {
            config.levels[target] = 'debug';
        });
    }
    return config;
}

// Load the config from the filesystem and the DEBUG environment,
// letting the latter take precedence if there are any specific target
// collisions.
function read_config() {
    return _.extend({}, read_config_file(), parse_debug_env());
}

module.exports = read_config;
