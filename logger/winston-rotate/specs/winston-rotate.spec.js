var _ = require('underscore');
var path = require('path');
var fs = require('fs');
var temp = require('temp');

var retry = require('bluebird-retry');

var testutils = require('testutils');
var expect = require('chai').expect;

var Rotate = require('../winston-rotate').Rotate;

testutils.mode.server();

temp.track();

describe('winston rotate tests', function () {

    var old_stdio = null;
    var old_stderr = null;

    before(function (done) {
        old_stdio = process.stdout;
        old_stderr = process.stderr;

        done();
    });

    after(function (done) {
        process.__defineGetter__('stdout', function () {
            return old_stdio;
        });
        process.__defineGetter__('stderr', function () {
            return old_stderr;
        });

        done();
    });

    it('throws an error if a log file is not specified', function (done) {
        var fn = function () {
            new Rotate({
                size: '1k',
                keep: 2
            });
        };
        expect(fn).to.throw(Error);
        done();
    });

    for (var iteration = 0; iteration < 10; iteration++) {
        it('can log a basic message ' + iteration, function (done) {
            var temp_dir = temp.mkdirSync('winston-rotate-test');
            var file = path.join(temp_dir, 'rotate.log');
            var logger = new Rotate({
                file: file,
                size: '1k',
                keep: 1
            });

            // make sure basic logging functionality is working
            logger.log('info', 'logged', {}, function () {
                var contents = fs.readFileSync(file, 'utf8');
                expect(contents).to.equal('info: logged\n');
                logger.close();
                done();
            });
        });
    }

    it('rotates after size is exceeded', function (done) {
        var temp_dir = temp.mkdirSync('winston-rotate-test');

        var file = path.join(temp_dir, 'rotate.log');
        var logger = new Rotate({
            file: file,
            size: '1k',
            keep: 2
        });

        logger.once('error', done);
        logger.once('rotated', function (rotated_file) {
            expect(rotated_file).to.equal(temp_dir + '/rotate.log.0.gz');

            logger.once('ready', function () {
                var files = fs.readdirSync(temp_dir);
                expect(files.length).to.equal(2);
                expect(_.include(files, 'rotate.log')).to.be.true;

                // there is a brief delay before the log line gets written,
                // hence the retry
                retry(function () {
                    var stats = fs.statSync(file);

                    // 24 bytes + 'info:' prefix + new line
                    expect(stats.size).to.equal(31);
                }, {
                    interval: 200,
                    backoff: 1,
                    max_tries: 3
                }).done(done, done);

            });
        });

        // create 1k buffer, enough to rotate the log
        var buff = new Buffer(1000);
        buff.fill('-');

        logger.log('info', buff.toString(), {}, function () {
            var files = fs.readdirSync(temp_dir);
            expect(files.length).to.equal(1);
            expect(_.first(files)).to.equal('rotate.log');

            buff = new Buffer(24);
            buff.fill('-');

            // should trigger a rotation
            logger.log('info', buff.toString(), {}, function () {
            });
        });
    });

    it('expunges old logs after keep is exceeded', function (done) {
        var temp_dir = temp.mkdirSync('winston-rotate-test');
        var file = path.join(temp_dir, 'rotate.log');
        var logger = new Rotate({
            file: file,
            size: '1k',
            keep: 2
        });

        var rotation_count = 0;
        var ready_count = 0;

        logger.once('error', done);
        logger.on('rotated', function () {
            rotation_count++;
        });

        logger.on('ready', function () {
            ready_count++;
            if (ready_count === 6) {
                expect(rotation_count).to.equal(5);

                // should have at most 2
                var files = fs.readdirSync(temp_dir);
                expect(files.length).to.equal(2);
                done();
            }
        });

        var buff = new Buffer(1000);
        buff.fill('-');

        var recursive_log = function (n) {
            if (n === 0) {
                return;
            }
            logger.log('info', buff.toString(), {}, function () {
                recursive_log(--n);
            });
        };

        recursive_log(6);
    });
});
