var testutils = require('testutils');
var expect = require('chai').expect;
var import_utils = require('test-utils/import-utils');
var orestesUtils = require('../../orestes/orestes-utils');

testutils.mode.server();
testutils.depends('data-engine', 'service-auth', 'service-deployment');

var cassandra_client;
var keyspace = orestesUtils.orestesKeyspaceName('client_test');

describe('make sure the Cassandra client is functional', function() {
    before(function() {
        return import_utils.get_cassandra_client()
            .then(function(client) {
                cassandra_client = client;
                return cassandra_client.createKeyspaceIfNotExists(keyspace);
            })
            .then(function() {
                return cassandra_client.createTableIfNotExists(keyspace + '.test', {x: 'int'}, 'x');
            });
    });

    it('finds a table that exists', function() {
        return cassandra_client.checkTableExists(keyspace + '.test');
    });

    it('does not find a table that does not exist (nonexistent table name)', function() {
        return cassandra_client.checkTableExists(keyspace + '.bananas')
        .then(function() {
            throw new Error('should have failed');
        })
        .catch(function(err) {
            expect(err.message).equal('unconfigured columnfamily bananas');
        });
    });

    it('does not find a table that does not exist (nonexistent space name)', function() {
        return cassandra_client.checkTableExists('"bananas".test')
        .then(function() {
            throw new Error('should have failed');
        })
        .catch(function(err) {
            expect(err.message).equal('Keyspace bananas does not exist');
        });
    });
});
