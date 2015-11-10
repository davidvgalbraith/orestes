var Promise = require('bluebird');
var fse = Promise.promisifyAll(require('fs-extra'));
var express = require('express');
var body_parser = require('body-parser');
var retry = require('bluebird-retry');
var CassandraClient = require('./cassandra').client;
var CassandraErrors = require('./cassandra').errors;
var Elasticsearch = require('./elasticsearch');
var Query = require('./orestes-query');
var Delete = require('./orestes-remover');
var Insert = require('./orestes-inserter');
var utils = require('./orestes-utils');
var logger = require('logger').get('orestes');
var Bubo = require('./bubo');

var cassandra_client;
var SUCCESS = 200;

function _connect_to_cassandra(parameters) {
    return retry(function() {
        return cassandra_client.connect(parameters)
        .catch(function(err) {
            logger.info('waiting for cassandra to start...', {message: err.message});
            throw err;
        });
    }, { max_tries: Infinity });
}

function _connect_to_elasticsearch() {
    var elasticsearch = new Elasticsearch(config);
    return elasticsearch.startup();
}

function startup(config) {
    cassandra_client = new CassandraClient(config.get('cassandra'));

    var cass_params = {
        address: config.get('cassandra').host,
        port: config.get('cassandra').native_transport_port
    };

    var es_params = {

    };
    return Promise.all([
        _connect_to_cassandra(cass_params),
        _connect_to_elasticsearch()
    ])
    .then(function() {
        var bubo_cache = new Bubo(utils.buboOptions);

        Query.init(config);
        Delete.init(config, cassandra_client);
        Insert.init(config, bubo_cache, cassandra_client);
        utils.init(config, cassandra_client, bubo_cache);
        return _init_routes(config);
    });
}

function _init_routes(config) {
    var app = express();
    app.post('/write/:space', body_parser.json(), function write(req, res, next) {
        console.error('write is Miami 911');
        res.sendStatus(SUCCESS);
    });

    app.post('/read/:space', body_parser.json(), function write(req, res, next) {
        console.error('read is Miami 911');
        res.sendStatus(SUCCESS);
    });

    return new Promise(function(resolve, reject) {
        app.listen(config.port, function() {
            console.log('Orestes is online!');
            resolve();
        });
    });
}

function get_inserter(options) {
    return new Insert.inserter(options);
}

if (process.env.MAIN || require.main === module) {
    fse.readJsonAsync('../conf/orestes-config.json')
        .then(function(conf) {
            return startup(conf);
        });
}

module.exports = {
    init: init,
    startup: startup,
    get_inserter: get_inserter,
    remove: Delete.remove,

    count_points: Query.count_points,
    get_metric_streams: Query.get_metric_streams,
    get_stream_list: Query.get_stream_list,
    get_stream_list_opt: Query.get_stream_list_opt,
    get_read_stats: Query.get_read_stats
};
