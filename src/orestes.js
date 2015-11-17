var Promise = require('bluebird');
var fse = Promise.promisifyAll(require('fs-extra'));
var express = require('express');
var body_parser = require('body-parser');
var Long = require('long');
var util = require('util');
var retry = require('bluebird-retry');
var error_handler = require('./middleware/error-handler');
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
var ES_MATCH_ALL = {
    match_all: {}
};

function _connect_to_cassandra(config) {
    var cass_params = {
        address: config.cassandra.host,
        port: config.cassandra.native_transport_port
    };

    return retry(function() {
        return cassandra_client.connect(cass_params)
        .catch(function(err) {
            logger.info('waiting for cassandra to start...', {message: err.message});
            throw err;
        });
    }, { max_tries: Infinity });
}

function _connect_to_elasticsearch(config) {
    var elasticsearch = new Elasticsearch(config);
    return elasticsearch.startup();
}

function init(config) {
    Elasticsearch.init(config);
    cassandra_client = new CassandraClient(config.cassandra);
    var bubo_cache = new Bubo(utils.buboOptions);

    Query.init(config);
    Delete.init(config, cassandra_client);
    Insert.init(config, bubo_cache, cassandra_client);
    utils.init(config, cassandra_client, bubo_cache);
}

function startup(config) {
    init(config);
    return Promise.all([
        _connect_to_cassandra(config),
        _connect_to_elasticsearch(config)
    ])
    .then(function() {
        return _init_routes(config);
    });
}

function _init_routes(config) {
    var app = express();
    app.post('/write/:space*?', body_parser.json(), function write(req, res, next) {
        var points = req.body;
        var space = req.params.space || 'default';

        return Insert.insert(points, space)
            .then(function(result) {
                res.json(result);
            })
            .catch(function(err) {
                return next(err);
            });
    });

    app.post('/read/:space*?', body_parser.json(), function read(req, res, next) {
        var space = req.params.space || 'default';
        var es_filter = req.body.query || ES_MATCH_ALL;
        var startMs = new Date(req.body.start || 0).getTime();
        var endMs = new Date(req.body.end || Date.now()).getTime();
        var options = {
            series_limit: req.body.series_limit,
            points_limit: req.body.points_limit
        };

        var first = true;
        res.write('{"series":[');
        function write_series(series) {
            if (first) {
                first = false;
            } else {
                res.write(',');
            }
            res.write(JSON.stringify(series));
        }

        function fetch_series(fetcher) {
            var points = [];
            function loop() {
                return fetcher.fetch(-1)
                    .then(function(result) {
                        points = points.concat(result.points);
                        if (!result.eof) {
                            return loop();
                        }
                    });
            }

            return loop()
                .then(function() {
                    write_series({
                        tags: fetcher.tags,
                        points: points
                    });
                });
        }

        function fetch_counts(fetcher) {
            var count = 0;
            function loop() {
                return fetcher.fetch(-1)
                    .then(function(result) {
                        var count_object = result.points[0];
                        count += new Long(count_object.low, count_object.high).toInt();
                        if (!result.eof) {
                            return loop();
                        }
                    });
            }

            return loop()
                .then(function() {
                    write_series({
                        tags: fetcher.tags,
                        count: count
                    });
                });
        }

        var read_promise;
        var aggregations = req.body.aggregations;
        if (aggregations) {
            if (aggregations.length === 1 && aggregations[0].type === 'count') {
                read_promise = Query.count_points(es_filter, space, startMs, endMs, fetch_counts);
            } else {
                var err = new Error('the only supported aggregation type is count');
                err.status = 400;
                return next(err);
            }
        } else {
            read_promise = Query.read(es_filter, space, startMs, endMs, options, fetch_series);
        }

        return read_promise
            .then(function() {
                res.end(']}');
            })
            .catch(function(err) {
                logger.error(err.stack);
                var terminator = util.format('], "error": "%s"}', err.message);
                res.end(terminator);
            });
    });

    app.post('/series/:space*?', body_parser.json(), function streams(req, res, next) {
        var space = req.params.space || 'default';
        var es_filter = req.body.query || ES_MATCH_ALL;
        var startMs = new Date(req.body.start || 0).getTime();
        var endMs = new Date(req.body.end || Date.now()).getTime();

        res.write('{"series":[');
        var first = true;
        function process_streams(streams) {
            if (first) {
                first = false;
            } else {
                res.write(',');
            }

            var json = JSON.stringify(streams);
            // write the objects in the array of streams
            // but not the "[" and "]" so we just get the top-level array
            res.write(json.substring(1, json.length-1));
        }

        return Query.get_stream_list(es_filter, space, startMs, endMs, process_streams)
            .then(function() {
                res.end(']}');
            })
            .catch(function(err) {
                logger.error(err.stack);
                var terminator = util.format('], "error": "%s"}', err.message);
                res.end(terminator);
            });
    });

    app.post('/select_distinct/:space*?', body_parser.json(), function select_distinct(req, res, next) {
        var space = req.params.space || 'default';
        var keys = req.body.keys;
        var es_filter = req.body.query || ES_MATCH_ALL;

        return Query.select_distinct(es_filter, space, keys)
            .then(function(result) {
                res.end(JSON.stringify(result));
            })
            .catch(function(err) {
                return next(err);
            });
    });

    app.post('/delete', body_parser.json(), function delete_data(req, res, next) {
        return Delete.remove(req.body)
            .then(function() {
                res.sendStatus(SUCCESS);
            })
            .catch(function(err) {
                return next(err);
            });
    });

    app.use(error_handler());

    return new Promise(function(resolve, reject) {
        app.listen(config.port, function() {
            console.log('Orestes is online!');
            resolve();
        });
    });
}

if (process.env.MAIN || require.main === module) {
    fse.readJsonAsync(__dirname + '/../conf/orestes-config.json')
        .then(function(conf) {
            return startup(conf);
        });
}

module.exports = {
    init: init,
    startup: startup,
    remove: Delete.remove,

    write: Insert.insert,
    read: Query.read,
    count_points: Query.count_points,
    get_stream_list: Query.get_stream_list,
    get_stream_list_opt: Query.get_stream_list_opt
};
