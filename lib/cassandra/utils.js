var Promise = require('bluebird');
var retry = require('bluebird-retry');
var _ = require('underscore');
var util = require('util');
var errors = require('./cassandra-errors');
var valid_space = /^[\w]+$/;

var logger = require('../../logger').get('cassandra-utils');

var cassandra_client, prepareds;

var pendingTables = {};

function init(cass_client, preparedQueries) {
    cassandra_client = cass_client;
    prepareds = preparedQueries;
}

function buildOptsString(tableOpts) {
    var strings = [];
    if (tableOpts.compact_storage) {
        strings.push('COMPACT STORAGE');
    }
    _.chain(tableOpts).omit('compact_storage').each(function(value, key) {
        strings.push(key + '=' + JSON.stringify(value));
    });

    return 'WITH ' + replaceAll(strings.join(' AND '), '"', "'") + ';';
}

function replaceAll(string, replacee, replacer) {
    while (string.indexOf(replacee) !== -1) {
        string = string.replace(replacee, replacer);
    }
    return string;
}

function validateHasAll(options, required) {
    var missing = [];
    required.forEach(function(option) {
        if (!options[option]) {
            missing.push(option);
        }
    });

    if (missing.length > 0) {
        throw new Error('missing required keys: ' + JSON.stringify(missing));
    }
}

function createTable(options) {
    validateHasAll(options, ['keyspace', 'columnfamily', 'table_fields', 'primary_key', 'table_options']);
    var fullName = options.keyspace + '.' + options.columnfamily;
    if (!pendingTables[fullName]) {
        pendingTables[fullName] = cassandra_client.createKeyspaceIfNotExists(options.keyspace, 1)
            .then(function() {
                return cassandra_client.createTableIfNotExists(fullName, options.table_fields, options.primary_key, options.table_options);
            })
            .catch(function(err) {
                throw errors.categorize_error(err, 'create_table');
            })
            .finally(function() {
                pendingTables[fullName] = null;
            });
    }

    return pendingTables[fullName];
}

function getPrepared(options) {
    validateHasAll(options, ['keyspace', 'columnfamily', 'table_fields', 'primary_key', 'table_options', 'cql']);
    var keyspace = options.keyspace;
    var cfName = options.columnfamily;
    var cql = util.format(options.cql, keyspace, cfName);

    if (!prepareds[cql]) {
        prepareds[keyspace] = {};
    }

    prepareds[cql] = ensureTable(options)
        .then(function() {
            return cassandra_client.prepare(cql);
        })
        .catch(function(err) {
            prepareds[cql] = null;
            throw err;
        });

    return prepareds[cql];
}

function ensureTable(options) {
    var keyspace = options.keyspace;
    var cfName = options.columnfamily;
    var fullTableName = keyspace + '.' + cfName;
    if (!valid_space.test(keyspace.substring(1, keyspace.length - 1))) {
        logger.warn('invalid space', keyspace);
        return Promise.reject(new Error('invalid space ' + keyspace));
    }

    return createTable(options)
    .catch(function(err) {
        logger.error('failed to create table', fullTableName, err);
        throw err;
    });
}

function _execute_query(q, opts, cb) {
    q.execute(opts, cb);
}

var execute_query = Promise.promisify(_execute_query);

function runCql(cql) {
    return cassandra_client.execute(cql);
}

function getAllTablesForKeyspace(keyspace) {
    return cassandra_client.getAllTablesForSpace(keyspace);
}

function awaitTable(table) {
    return pendingTables[table] || Promise.resolve();
}

module.exports = {
    init: init,
    buildOptsString: buildOptsString,
    createTable: createTable,
    getPrepared: getPrepared,
    execute_query: execute_query,
    runCql: runCql,
    getAllTablesForKeyspace: getAllTablesForKeyspace,
    validateHasAll: validateHasAll,
    awaitTable: awaitTable,
};
