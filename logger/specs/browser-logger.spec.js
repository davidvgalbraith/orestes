var moment = require('moment');
var testutils = require('testutils');
var expect = require('chai').expect;

var Logger = require('../logger');

testutils.mode.browser();

var log_level;

describe('browser logger tests', function () {
    this.timeout(500);

    // hijack console
    var orig_console_info = console.info;
    var orig_console_debug = console.debug;
    var orig_console_warn = console.warn;
    var orig_console_error = console.error;

    before(function (done) {
        // capture default level before setting in unit tests
        log_level = Logger.get_default_level();

        done();
    });

    after(function (done) {
        // restore default level to what it was before the test
        Logger.set_level('*', log_level);

        // set back console functions
        console.info = orig_console_info;
        console.debug = orig_console_debug;
        console.warn = orig_console_warn;
        console.error = orig_console_error;

        done();
    });

    it('can log at a warning level', function (done) {
        console.warn = function () {
            var args = Array.prototype.slice.call(arguments, 0);

            var timestamp = moment(args[0]);
            expect(timestamp.isValid()).to.be.true;

            expect(args[1]).to.equal('-');
            expect(args[2]).to.equal('warn:');
            expect(args[3]).to.equal('[info-logger]');
            expect(args[4]).to.equal('hello');

            done();
        };

        var logger = Logger.get('info-logger');
        logger.warn('hello');
    });

    it('does emit logged events', function (done) {

        Logger.config({
            levels: {
                '*': 'info'
            }
        });

        // should default
        var logger = Logger.get('events-logged');
        expect(logger.level).to.equal(2);

        console.info = function () {
        };

        var not_called = function (err, level, msg, meta) {
            throw new Error('should not be called');
        };

        logger.on('logged', not_called);
        logger.debug('not called');

        logger.off('logged', not_called);

        var called = function (err, level, msg, meta) {
            expect(err).to.equal(null);

            expect(level).to.equal('info');
            expect(msg).a('array');

            expect(moment(msg[0]).isValid()).to.be.true;
            expect(msg[1]).to.equal('-');
            expect(msg[2]).to.equal('info:');
            expect(msg[3]).to.equal('[events-logged]');
            expect(msg[4]).to.equal('logged');

            expect(meta).a('object');

            done();
        };

        logger.on('logged', called);
        logger.info('logged');
    });

    it('does not log if the level (debug) is greater than the current threshold (info)', function (done) {
        console.info = function () {
            done();
        };
        console.debug = function () {
            throw new Error('should not be called');
        };

        Logger.config({
            levels: {
                '*': 'info'
            }
        });

        var logger = Logger.get('no-log');
        logger.debug('not shown');
        logger.info('is shown');
    });

    it('does log (debug) if the threshold is changed', function (done) {
        console.debug = function () {
            done();
        };

        Logger.config({
            levels: {
                '*': 'debug'
            }
        });

        var logger = Logger.get('does-log');
        expect(logger.level).to.equal(3);

        logger.debug('is shown');
    });

    it('does not log if the threshold is changed back', function (done) {
        console.debug = function () {
            throw new Error('should not be called');
        };

        Logger.config({
            levels: {
                '*': 'info'
            }
        });

        var logger = Logger.get('threshold-altered');
        expect(logger.level).to.equal(2);

        logger.debug('not shown');

        done();
    });

    it('can set a logger specific log level', function (done) {
        Logger.config({
            levels: {
                '*': 'info',
                'specific-logger': 'debug'
            }
        });

        var logger = Logger.get('general-logger');
        expect(logger.level).to.equal(2);

        console.debug = function () {
            throw new Error('should not be called');
        };

        logger.debug('not called');

        var specific = Logger.get('specific-logger');
        expect(specific.level).to.equal(3);

        console.debug = function () {
            done();
        };

        specific.debug('called');
    });

    it('can set a logger specific log level using a prefix', function (done) {
        Logger.config({
            levels: {
                '*': 'info',
                'specific': 'debug'
            }
        });

        var logger = Logger.get('general-logger');
        expect(logger.level).to.equal(2);

        console.debug = function () {
            throw new Error('should not be called');
        };

        logger.debug('not called');

        var specific = Logger.get('specific-logger');
        expect(specific.level).to.equal(3);

        console.debug = function () {
            done();
        };

        specific.debug('called');
    });

    it('can hot update the level for all loggers', function (done) {
        Logger.config({
            levels: {
                '*': 'info'
            }
        });

        var logger = Logger.get('hot-update');
        expect(logger.level).to.equal(2);

        console.debug = function () {
            throw new Error('should not be called');
        };

        logger.debug('not called');

        // hot update
        Logger.config({
            levels: {
                '*': 'debug'
            }
        });

        expect(logger.level).to.equal(3);

        console.debug = function () {
            done();
        };

        logger.debug('called');
    });

    it('can hot update the level for a specific logger', function (done) {
        Logger.config({
            levels: {
                '*': 'error',
                'hot-update-specific': 'info'
            }
        });

        var logger = Logger.get('hot-update-specific');
        expect(logger.level).to.equal(2);

        console.debug = function () {
            throw new Error('should not be called');
        };

        logger.debug('not called');

        // hot update
        Logger.config({
            levels: {
                '*': 'error',
                'hot-update-specific': 'debug'
            }
        });

        expect(logger.level).to.equal(3);

        console.debug = function () {
            done();
        };

        logger.debug('called');
    });

    it('specific logger uses * level if the * threshold is greater', function (done) {
        Logger.config({
            levels: {
                '*': 'info',
                'use-star': 'error'
            }
        });

        var logger = Logger.get('use-star');
        expect(logger.level).to.equal(2);

        console.info = function () {
            done();
        };

        logger.info('called');
    });

    it('can call config with no argument', function (done) {
        Logger.config();

        // should default
        var logger = Logger.get('no-config');
        expect(logger.level).to.equal(2);

        console.info = function () {
            done();
        };

        logger.info('called');
    });

    it('can specify empty config', function (done) {
        Logger.config({});

        // should default
        var logger = Logger.get('empty-config');
        expect(logger.level).to.equal(2);

        console.info = function () {
            done();
        };

        logger.info('called');
    });

    it('can reset config', function (done) {
        Logger.config({
            levels: {
                '*': 'error'
            }
        });

        var logger = Logger.get('reset-config');
        expect(logger.level).to.equal(0);

        console.info = function () {
            throw new Error('should not be called');
        };

        logger.info('not called');

        // hot reset to defaults
        Logger.reset();

        console.info = function () {
            done();
        };

        logger.info('called');
    });

    it('can log an error', function (done) {
        console.error = function () {
            var args = Array.prototype.slice.call(arguments, 0);

            var timestamp = moment(args[0]);
            expect(timestamp.isValid()).to.be.true;

            expect(args[1]).to.equal('-');
            expect(args[2]).to.equal('error:');
            expect(args[3]).to.equal('[ex]');

            expect(args[4]).to.equal('fails with error');
            expect(args[5]).a('object');
            expect(args[5].message).to.equal('bad');

            done();
        };

        var logger = Logger.get('ex');

        var error = new Error('bad');
        logger.error('fails with error', error);
    });

    it('can log an error without a stack trace', function (done) {
        console.error = function () {
            var args = Array.prototype.slice.call(arguments, 0);

            var timestamp = moment(args[0]);
            expect(timestamp.isValid()).to.be.true;

            expect(args[1]).to.equal('-');
            expect(args[2]).to.equal('error:');
            expect(args[3]).to.equal('[ex-no-st]');

            expect(args[4]).to.equal('fails with error');
            expect(args[5]).a('object');
            expect(args[5].message).to.equal('the error message');

            done();
        };

        var logger = Logger.get('ex-no-st');

        var error = new Error('the error message');
        error.stack = null;

        logger.error('fails with error', error);
    });

    it('can log an error with a stack trace', function (done) {
        console.error = function () {
            var args = Array.prototype.slice.call(arguments);

            var timestamp = moment(args[0]);
            expect(timestamp.isValid()).to.be.true;

            expect(args[1]).to.equal('-');
            expect(args[2]).to.equal('error:');
            expect(args[3]).to.equal('[ex-w-st]');

            done();
        };

        var logger = Logger.get('ex-w-st');

        var error = new Error('bad');
        logger.error(error.stack);
    });

    it('can log an error with a stack trace and message', function (done) {
        console.error = function () {
            var args = Array.prototype.slice.call(arguments);

            var timestamp = moment(args[0]);
            expect(timestamp.isValid()).to.be.true;

            expect(args[1]).to.equal('-');
            expect(args[2]).to.equal('error:');
            expect(args[3]).to.equal('[ex-w-st-and-msg]');

            done();
        };

        var logger = Logger.get('ex-w-st-and-msg');

        var error = new Error('bad');
        logger.error('fails with stack trace', error.stack);
    });
});
