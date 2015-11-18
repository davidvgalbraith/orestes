var _ = require('underscore');
var os = require('os');
var fs = require('fs');
var moment = require('moment');
var testutils = require('testutils');
var expect = require('chai').expect;
var util = require('util');

var Logger = require('../logger');

testutils.mode.server();

var log_level;

var LOG_FILES = [];

function logFile() {
    var date = new Date();
    var name = ['logger-test', date.getTime()].join('-');
    var file = [os.tmpdir(), name].join('/');

    LOG_FILES.push(file);
    return file;
}

function format_log(name, msg) {
    return util.format("[%s] (%d) %s", name, process.pid, msg);
}

describe('server logger tests', function () {
    this.timeout(500);

    // skip these tests if debugging is enabled
    this.pending = process.env.DEBUG ? true : false;

    before(function (done) {
        // capture default level before setting in unit tests
        log_level = Logger.get_default_level();

        done();
    });

    after(function (done) {
        // restore default level to what it was before the test
        Logger.set_level('*', log_level);

        Logger.reset();

        var errors = false;

        function doDelete(file) {
            try {
                if (fs.existsSync(file)) {
                    fs.unlinkSync(file);
                }
            } catch (ex) {
                console.error('could not delete', file, ex.stack);
                errors = true;
            }
        }

        _.each(LOG_FILES, doDelete);

        if (errors) {
            return done(new Error('could not delete one or more log files'));
        } else {
            done();
        }

    });

    it('can log using a default transport', function (done) {
        var logger = Logger.get('dt');
        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('info');
            expect(msg).to.equal(format_log('dt', 'hello'));
            expect(meta).to.a('object');
            expect(_.isEmpty(meta)).to.be.true;
            done();
        });

        logger.info('hello');
    });

    it('log function can take multiple arguments', function (done) {
        var bar = 'bar';

        var logger = Logger.get('multiple-args');
        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('info');
            expect(msg).to.equal(format_log('multiple-args', 'foo bar baz'));
            expect(meta).to.a('object');
            expect(_.isEmpty(meta)).to.be.true;
            done();
        });

        logger.info('foo', bar, 'baz');
    });

    it('can log a function', function (done) {
        var fn = function () {
            this.foo = 'bar';
        };

        var logger = Logger.get('logged-fn');
        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('info');
            expect(msg).to.match(/\[logged-fn\] \(\d+\) a function([\s\S]*)this\.foo(\s|)=(\s|)'bar'/m);
            expect(meta).to.a('object');
            expect(_.isEmpty(meta)).to.be.true;
            done();
        });

        // fn is not treated as a winston callback
        logger.info('a function', fn);
    });

    it('can log an object', function (done) {
        var obj = {
            foo: 'jut',
            bar: 1337
        };

        var logger = Logger.get('logged-obj');
        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('info');
            expect(msg).to.equal(format_log('logged-obj', 'an object'));
            expect(meta).to.a('object');

            expect(meta.foo).to.equal('jut');
            expect(meta.bar).to.equal(1337);

            done();
        });

        logger.info('an object', obj);
    });

    it('can log multiple objects', function (done) {
        var obj1 = {
            foo: 'jut1',
            bar: 1337
        };

        var obj2 = {
            baz: 'jut2'
        };

        var logger = Logger.get('logged-objs');
        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('info');
            expect(msg).to.equal(format_log('logged-objs', 'objects { foo: \'jut1\', bar: 1337 }'));
            expect(meta).to.a('object');

            expect(meta.baz).to.equal('jut2');

            done();
        });

        logger.info('objects', obj1, obj2);
    });

    it('does not log if the level (debug) is greater than the current threshold (info)', function (done) {
        var logger = Logger.get('no-log');

        var not_logged = function (err, level, msg, meta) {
            throw new Error('should not be called');
        };

        logger.once('logged', not_logged);
        logger.debug('not shown');

        logger.removeListener('logged', not_logged);

        logger.once('logged', function (err, level, msg, meta) {
            done();
        });
        logger.info('hello');
    });

    it('does log (debug) if the threshold is changed', function (done) {
        Logger.config({
            levels: {
                '*': 'debug'
            }
        });

        var logger = Logger.get('does-log');
        expect(logger.level).to.equal(3);

        logger.once('logged', function (err, level, msg, meta) {
            expect(level).to.equal('debug');
            expect(msg).to.equal(format_log('does-log', 'is shown'));

            done();
        });
        logger.debug('is shown');
    });

    it('does not log if the threshold is changed back', function (done) {
        Logger.config({
            levels: {
                '*': 'info'
            }
        });

        var logger = Logger.get('threshold-altered');
        expect(logger.level).to.equal(2);

        logger.once('logged', function (err, level, msg, meta) {
            throw new Error('should not be called');
        });
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

        logger.once('logged', function (err, level, msg, meta) {
            throw new Error('should not be called');
        });
        logger.debug('not called');

        var specific = Logger.get('specific-logger');
        expect(specific.level).to.equal(3);

        specific.once('logged', function (err, level, msg, meta) {
            done();
        });
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

        logger.once('logged', function (err, level, msg, meta) {
            throw new Error('should not be called');
        });
        logger.debug('not called');

        var specific = Logger.get('specific-logger');
        expect(specific.level).to.equal(3);

        specific.once('logged', function (err, level, msg, meta) {
            done();
        });
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

        var not_logged = function (err, level, msg, meta) {
            throw new Error('should not be called');
        };

        logger.once('logged', not_logged);
        logger.debug('not called');

        logger.removeListener('logged', not_logged);

        // hot update
        Logger.config({
            levels: {
                '*': 'debug'
            }
        });

        expect(logger.level).to.equal(3);

        logger.once('logged', function (err, level, msg, meta) {
            done();
        });
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

        var not_logged = function (err, level, msg, meta) {
            throw new Error('should not be called');
        };

        logger.once('logged', not_logged);
        logger.debug('not called');

        logger.removeListener('logged', not_logged);

        // hot update
        Logger.config({
            levels: {
                '*': 'error',
                'hot-update-specific': 'debug'
            }
        });

        expect(logger.level).to.equal(3);

        logger.once('logged', function (err, level, msg, meta) {
            done();
        });
        logger.debug('called');
    });

    it('can set a log level for all loggers', function (done) {
        var not_logged = function (err, level, msg, meta) {
            throw new Error('should not be called');
        };

        var logger1 = Logger.get('al-l1');
        logger1.once('logged', not_logged);

        var logger2 = Logger.get('al-l2');

        logger2.once('logged', not_logged);

        logger1.debug('not shown');
        logger2.debug('not shown');

        logger1.removeListener('logged', not_logged);
        logger2.removeListener('logged', not_logged);

        Logger.set_level('*', 'debug');

        var count = 0;
        var logged = function () {
            count++;
            if (count === 2) {
                done();
            }
        };

        logger1.once('logged', logged);
        logger2.once('logged', logged);

        logger1.debug('shown');
        logger2.debug('shown');
    });

    it('can set a log level for a specific logger', function (done) {
        var not_logged = function (err, level, msg, meta) {
            throw new Error('should not be called');
        };

        var logger1 = Logger.get('sp-l1');
        logger1.once('logged', not_logged);

        var logger2 = Logger.get('sp-l2');

        logger2.once('logged', not_logged);

        logger1.debug('not shown');
        logger2.debug('not shown');

        logger2.removeListener('logged', not_logged);

        var logged = function () {
            done();
        };

        logger2.once('logged', logged);

        Logger.set_level('sp-l2', 'debug');

        logger1.debug('not shown');
        logger2.debug('shown');
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

        logger.once('logged', function (err, level, msg, meta) {
            done();
        });
        logger.info('called');
    });

    it('can call config with no argument', function (done) {
        Logger.config();

        // should default
        var logger = Logger.get('no-config');
        expect(logger.level).to.equal(2);

        logger.once('logged', function (err, level, msg, meta) {
            done();
        });
        logger.info('called');
    });

    it('can specify empty config', function (done) {
        Logger.config({});

        // should default
        var logger = Logger.get('empty-config');
        expect(logger.level).to.equal(2);

        logger.once('logged', function (err, level, msg, meta) {
            done();
        });
        logger.info('called');
    });

    it('can reset config', function (done) {
        Logger.config({
            levels: {
                '*': 'error'
            },
            transports: {
                console: {
                    colorize: false
                }
            }
        });

        var logger = Logger.get('reset-config');
        expect(logger.level).to.equal(0);

        var not_logged = function (err, level, msg, meta) {
            throw new Error('should not be called');
        };

        logger.once('logged', not_logged);
        logger.info('not called');

        logger.removeListener('logged', not_logged);

        // hot reset to defaults
        Logger.reset();

        logger.once('logged', function (err, level, msg, meta) {
            done();
        });
        logger.info('called');
    });

    it('can log an error', function (done) {
        var logger = Logger.get('ex');

        var error = new Error('bad');

        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('error');
            expect(msg).to.match(/^\[ex\] \(\d+\) fails with error/);

            // message includes the stack trace
            expect(msg).to.match(/logger.spec.js/);

            expect(meta).to.a('object');
            expect(_.isEmpty(meta)).to.be.true;

            done();
        });
        logger.error('fails with error', error);
    });

    it('can log an error without a stack trace', function (done) {
        var logger = Logger.get('ex-no-st');

        var error = new Error('the error message');
        error.stack = null;

        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('error');
            expect(msg).to.match(/^\[ex-no-st\] \(\d+\) fails with error Error: the error message/);

            expect(meta).to.a('object');
            expect(_.isEmpty(meta)).to.be.true;

            done();
        });
        logger.error('fails with error', error);
    });

    it('can log an error with a stack trace', function (done) {
        var logger = Logger.get('ex-w-st');

        var error = new Error('bad');

        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('error');

            expect(msg).to.match(/^\[ex-w-st\]/);
            // message includes the stack trace
            expect(msg).to.match(/logger.spec.js/);

            expect(meta).to.a('object');
            expect(_.isEmpty(meta)).to.be.true;

            done();
        });
        logger.error(error.stack);
    });

    it('can log an error with a stack trace and message', function (done) {
        var logger = Logger.get('ex-w-st-and-msg');

        var error = new Error('bad');

        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);
            expect(level).to.equal('error');

            expect(msg).to.match(/^\[ex-w-st-and-msg\] \(\d+\) fails with stack trace/);
            // message includes the stack trace
            expect(msg).to.match(/logger.spec.js/);

            expect(meta).to.a('object');
            expect(_.isEmpty(meta)).to.be.true;

            done();
        });
        logger.error('fails with stack trace', error.stack);
    });

    // XXX/lomew this seems racy.  Might need some sort of close/flush
    // before we go read the file.
    it('can specify a transport (file)', function (done) {
        var file = logFile();

        Logger.config({
            levels: {
                '*': 'info'
            },
            transports: {
                file: {
                    json: true,
                    filename: file
                }
            }
        });

        var logger = Logger.get('trans');

        logger.debug('not in file');

        logger.error('error in file');
        logger.warn('warning in file');

        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);

            function checkLine(data, level, name, msg) {
                var line = JSON.parse(data);

                expect(line.level).to.equal(level);
                expect(line.message).to.equal(format_log(name, msg));

                var timestamp = moment(line.timestamp);
                expect(timestamp.isValid()).to.equal(true);
            }

            function onRead(err, data) {
                if (err) {
                    return done(err);
                }

                var lines = data.split('\n');
                expect(lines.length).to.equal(4);

                checkLine(lines[0], 'error', 'trans', 'error in file');
                checkLine(lines[1], 'warn', 'trans', 'warning in file');
                checkLine(lines[2], 'info', 'trans', 'info in file');
                expect(lines[3]).to.equal('');

                done();
            }

            // make sure message was written
            fs.readFile(file, 'utf8', onRead);
        });

        logger.info('info in file');
    });

    it('has an expected log format', function (done) {
        var file = logFile();

        Logger.config({
            transports: {
                file: {
                    json: false,
                    filename: file
                }
            }
        });

        var logger = Logger.get('expected-format');

        logger.once('logged', function (err, level, msg, meta) {
            expect(err).to.equal(null);

            function onRead(err, data) {
                if (err) {
                    return done(err);
                }

                // {timestamp} - {level}: [{name}] ({pid}) message
                // example: 2014-04-09T22:21:33.634Z - info: [expected-format] (1234) hello
                expect(data).to.match(/^(.+) - info: \[expected-format\] \(\d+\) hello\n$/);

                done();
            }

            // make sure message was written
            fs.readFile(file, 'utf8', onRead);
        });
        logger.info('hello');
    });

    it('can run a query against a file transport', function (done) {
        var file = logFile();

        Logger.config({
            levels: {
                '*': 'info'
            },
            transports: {
                file: {
                    json: true,
                    filename: file
                }
            }
        });

        function onLogged() {
            var yesterday = new Date() - 24 * 60 * 60 * 1000;
            var now = new Date();

            var options = {
                from: yesterday,
                until: now,
                limit: 10,
                start: 0,
                order: 'desc',
                fields: ['level', 'message', 'timestamp']
            };

            function onQueried(err, results) {
                if (err) {
                    return done(err);
                }

                expect(results).a('object');

                var file = results.file;
                expect(file).a('array');

                expect(file.length).to.equal(1);

                var record = _.first(file);
                expect(record).a('object');

                expect(record.level).to.equal('info');
                expect(record.message).to.equal(format_log('query', 'hello'));
                expect(moment(record.timestamp).isValid()).to.be.true;

                done();
            }

            Logger.query(options, onQueried);
        }

        var logger = Logger.get('query');

        logger.once('logged', onLogged);
        logger.info('hello');
    });
});
