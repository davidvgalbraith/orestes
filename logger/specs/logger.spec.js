var testutils = require('testutils');
var expect = require('chai').expect;

var helpers = require('./helpers');

var Logger = require('../logger');

testutils.mode.server();

var log_level;

describe('logger tests', function () {

    before(function() {
        // capture default level before setting in unit tests
        log_level = Logger.get_default_level();
    });

    after(function() {
        // restore default level to what it was before the test
        Logger.set_level('*', log_level);
    });

    it('has expected api functions', function() {
        expect(Logger.get).a('function');
        expect(Logger.get_unique).a('function');
        expect(Logger.config).a('function');
        expect(Logger.reset).a('function');
    });

    it('can instantiate a logger', function() {
        var name = helpers.logger_name();
        var logger = Logger.get(name);
        helpers.check_logger(logger);

        expect(logger.name).to.equal(name);
    });

    it('can instantiate a unique logger', function() {
        var logger1 = Logger.get_unique('logger');
        helpers.check_logger(logger1);

        expect(logger1.name).to.match(/^logger/);
        expect(logger1.name).to.not.equal('logger');

        var logger2 = Logger.get_unique('logger');
        helpers.check_logger(logger2);

        expect(logger2.name).to.match(/^logger/);
        expect(logger2.name).to.not.equal('logger');

        expect(logger1.name).to.not.equal(logger2.name);
    });

    it('can set a logger\'s level', function() {
        var logger = Logger.get_unique('logger');
        var should_not_log = 'If you\'re reading this, the Logger module is broken.';
        var should_log = 'Testing logger...this should appear 6 times, so far:';
        var n = 0;
        helpers.check_logger(logger);

        logger.debug(should_not_log);
        logger.info(should_log, ++n);
        logger.warn(should_log, ++n);
        logger.error(should_log, ++n);

        logger.setLevel('warn');
        logger.info(should_not_log);
        logger.debug(should_not_log);
        logger.warn(should_log, ++n);
        logger.error(should_log, ++n);

        logger.setLevel('error');
        logger.info(should_not_log);
        logger.warn(should_not_log);
        logger.debug(should_not_log);
        logger.error(should_log, ++n);
    });
});
