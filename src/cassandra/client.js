var Base = require('extendable-base');
var cassandra = require('cassandra-native-driver');
var Promise = require('bluebird');
var Logger = require('logger');
var util = require('util');
var _ = require('underscore');
var dns = Promise.promisifyAll(require('dns'));
var net = require('net');
var types = cassandra.types;

var logger = Logger.get('cassandra-driver');
// these log messages are spammy and meaningless, so let's not print them
var IGNORED_LOG_REGEX = /had the following error on startup: 'Unable to connect'|Lost connection on host|Creating 1 IO worker threads|Connect error 'connection refused' on host/;

// Mapping from cassandra log levels to jut log levels
var levelmap = {
    'CRITICAL': 'error',
    'ERROR': 'error',
    'WARN': 'warn',
    'INFO': 'info',
    'DEBUG': 'debug',
    'TRACE': 'debug'
};

var _driver_log_initialized = false;

function log_init() {
    if (_driver_log_initialized) { return; }

    // Callback from cassandra for each log event. Since the various queues and
    // callbacks may add some delay between when the original event was logged
    // and the current time, print the delta.
    function driver_log(err, info) {
        var fn = logger[levelmap[info.severity]];
        if (!info.message.match(IGNORED_LOG_REGEX)) {
            fn(util.format('(-%dms) %s', new Date().getTime() - info.time_ms, info.message));
        }
    }
    cassandra.set_log_callback(driver_log);
    _driver_log_initialized = true;
}

// Wrapper around the cassandra client.
//
// Wraps a promisified client object and exposes additional helper methods for
// writing tests.
var CassandraClient = Base.extend({
    initialize: function(opts) {
        log_init();
        logger.info('configuring driver with options:', JSON.stringify(opts));
        this.opts = opts || {};
        this.client = new cassandra.Client(opts);
        Promise.promisifyAll(this.client);
    },

    connect: function(options) {
        // XXX the current cassandra driver gets confused with IPV6 addresses,
        // so resolve it explicitly as an IPV4 address here.
        var self = this;
        return Promise.try(function() {
            if (net.isIPv4(options.address)) {
                return [options.address];
            } else {
                return dns.lookupAsync(options.address, 4);
            }
        })
        .then(function(address) {
            options.address = address[0];
            return self.client.connectAsync(options);
        });
    },

    execute: function() {
        return this.client.executeAsync.apply(this.client, arguments);
    },

    query: function() {
        return this.client.queryAsync.apply(this.client, arguments);
    },

    prepare: function() {
        return this.client.prepareAsync.apply(this.client, arguments);
    },

    new_batch: function() {
        return this.client.new_batch.apply(this.client, arguments);
    },

    // create a keypsace with the given name and replication factor
    createKeyspaceIfNotExists: function(name, replication) {
        replication = replication || this.opts.cassandra_replicas || 1;

        var cql = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
            "{'class': 'SimpleStrategy', 'replication_factor' : %d};";

        return this.client.executeAsync(util.format(cql, name, replication));
    },

    cleanKeyspace: function(name) {
        return this.client.executeAsync('DROP KEYSPACE "' + name + '"')
        .catch(function(err) {
            if (/non existing keyspace/.test(err.message)) {
                return;
            }
            throw err;
        });
    },

    // Create a table with the given name in the current keyspace. Fields is an
    // object mapping the field name to the type.
    createTableIfNotExists: function(name, fields, key, opts) {
        opts = opts || "";
        var columns = _.map(fields, function(type, column) {
            return column + " " + type;
        });
        return this.execute(util.format("CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY(%s)) %s;",
        name, columns, key, opts));
    },

    getAllTablesForSpace: function(keyspace) {
        keyspace = keyspace.replace(/"/g, '\'');
        var cql = util.format("SELECT columnfamily_name FROM System.schema_columnfamilies WHERE keyspace_name=%s ALLOW FILTERING;", keyspace);
        return this.execute(cql)
            .then(function(result) {
                return _.pluck(result.rows, 'columnfamily_name');
            });
    },

    // check whether the given table exists
    checkTableExists: function(table) {
        var cql = util.format('SELECT * FROM %s LIMIT 1;', table);
        return this.execute(cql);
    }
},
{
    // Set the logging level for the driver (using cassandra's levels), which
    // both sets the internal driver logging level and the level of the jut
    // logger to match.
    set_log_level: function(level) {
        var jut_level = levelmap[level];
        if (!jut_level) {
            throw new Error('invalid logging level ' + level);
        }
        cassandra.set_log_level(level);
        Logger.set_level(logger.name, jut_level);
    }
});

CassandraClient.types = types;

module.exports = CassandraClient;
