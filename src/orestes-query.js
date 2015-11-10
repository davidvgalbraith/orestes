var _ = require('underscore');
var Promise = require('bluebird');
var request = require('request-async');
var Long = require('long');
var utils = require('./orestes-utils');
var cassUtils = require('../cassandra').utils;
var es_query = require('../electra/query-common');
var es_errors = require('../electra/es-errors');
var cass_errors = require('../cassandra').errors;
var errors = require('../data-service-errors');
var es_errors = require('../electra/es-errors');
var uncompactRows = utils.uncompactRows;
var aggregation = require('../electra/aggregation');
var Bubo = require('./bubo');
var sourceTypeFromESDoc = require('../electra/utils').sourceTypeFromESDoc;

var logger = require('logger').get('orestes');
var msInDay = 1000 * 60 * 60 * 24;
var msInWeek = msInDay * 7;

var es_url, METADATA_GRANULARITY, CLUSTER_SIZE, METADATA_FETCH_SIZE_FOR_COUNT;
var SAMPLE_RATE, MAX_SIMULTANEOUS_POINTS, METADATA_FETCH_SIZE, CONCURRENT_COUNTS;
var MAX_STREAMS_FOR_COUNT, MAX_STREAMS_FOR_READ;

// 30 minutes, make this configurable though
var RECENT_SLOP = 30 * 60 * 1000;

var buboResult = {};


function init(config) {
    es_url = 'http://' + config.get('elasticsearch').host + ':' + config.get('elasticsearch').port + '/';
    SAMPLE_RATE = config.get('sample_rate');
    MAX_SIMULTANEOUS_POINTS = config.get('orestes').max_simultaneous_points;
    MAX_STREAMS_FOR_READ = config.get('orestes').max_streams_for_read;
    METADATA_GRANULARITY = config.get('orestes').metadata_granularity_days;
    METADATA_FETCH_SIZE = config.get('orestes').metadata_fetch_size;
    CONCURRENT_COUNTS = config.get('orestes').max_concurrent_count_requests;
    MAX_STREAMS_FOR_COUNT = config.get('orestes').max_streams_for_count;
    METADATA_FETCH_SIZE_FOR_COUNT = config.get('orestes').metadata_fetch_size_for_count;
    // this is used to calculate the number of shards in our ES cluster
    // to calculate the fetch size for streams
    // if we have a cluster of hosts A, B, C and D, the ds_internal_addresses
    // for A will only have entries for B, C, and D, so we add one
    CLUSTER_SIZE = _.chain(config.get('ds_internal_addresses')).pluck('host').uniq().value().length + 1;
}

// To avoid having to increment statsd once for each request to cassandra, the
// logic in read allocates a stats tracker that is shared among the various
// streams and is periodically flushed to statsd.
function get_read_stats() {
    return {
        requests: 0,
        points: 0,

        report_metrics: function(metrics) {
            metrics.count('cassandra.request.op__select', this.requests, SAMPLE_RATE);
            metrics.count('cassandra.record.op__select', this.points, SAMPLE_RATE);
            this.requests = 0;
            this.points = 0;
        }
    };
}

function count_points(emit, es_filter, from, to, spaces, schemas, groupby, metrics, logger) {
    var fromMs = from ? from.milliseconds() : 0;
    var toMs = to ? to.milliseconds() : Date.now();
    var fromDay = Math.floor(fromMs / msInDay);
    var toDay = Math.floor(toMs / msInDay);
    var firstWeek = utils.roundToGranularity(fromDay);
    var lastWeek = utils.roundToGranularity(toDay);
    var options = {
        from: from,
        to: to,
        stream_limit: MAX_STREAMS_FOR_COUNT,
        streams_fetch_size: METADATA_FETCH_SIZE_FOR_COUNT,
        metrics: metrics
    };
    function add_counts(docs) {
        return Promise.map(docs, function(doc) {
            var day = utils.dayFromIndex(doc._index);
            var space = utils.spaceFromIndex(doc._index);
            var fromOffset = day === firstWeek ? fromMs % msInWeek : 0;
            var toOffset = day === lastWeek ? toMs % msInWeek : msInWeek;
            return utils.getPrepared(space, day, 'count')
                .then(function(prepared) {
                    var q = prepared.query();
                    q.bind([doc._id, fromOffset, toOffset], {});
                    return cassUtils.execute_query(q, {});
                })
                .then(function(result) {
                    var count_object = result.rows[0].count;
                    var count = new Long(count_object.low, count_object.high).toInt();
                    var groupby_fields = _.pick(doc._source, groupby);

                    emit(count, groupby_fields);
                });
        }, {concurrency: CONCURRENT_COUNTS});
    }
    return stream_metadata(add_counts, es_filter, spaces, schemas, options);
}
// Comments in juttle/read.js explain exactly what fetchers are.
function get_metric_streams(es_filter, from, to, spaces, schemas, metrics, logger) {

    metrics.count('orestes.request.op__read', 1, SAMPLE_RATE);
    var queries = {};
    function check_too_big(threshold) {
        // we want to give the user maximally detailed information about how
        // many streams they tried to query, but we don't know how many there
        // will be until we've built all the fetchers
        // so let's continue building the fetchers until we get MAX_SIMULTANEOUS_POINTS
        // of them (100k by default) even though we're going to reject the query if
        // there are more than MAX_STREAMS_FOR_READ of them (20k by default)
        // and report the real number of attempted streams if it's between
        // MAX_STREAMS_FOR_READ and MAX_SIMULTANEOUS_POINTS and just say "over {MAX_SIMULTANEOUS_POINTS}"
        // if it's too big for big data too big
        var size = _.size(queries);
        if (size > threshold) {
            metrics.increment('big_data_too_big.type__read_metrics');
            throw new errors.bigDataTooBig(null, null, {
                max: MAX_STREAMS_FOR_READ,
                received: threshold === MAX_SIMULTANEOUS_POINTS ? ('over ' + size) : size
            });
        }
    }
    var fromMs = from.milliseconds();
    var toMs = to ? to.milliseconds() : Date.now();
    function build_queries(docs) {
        check_too_big(MAX_SIMULTANEOUS_POINTS);
        metrics.count('elasticsearch.record.op__search_metadata', docs.length, SAMPLE_RATE);
        return buildQueries(docs, fromMs, toMs, queries);
    }
    return stream_metadata(build_queries, es_filter, spaces, schemas, {from: from, to: to, metrics: metrics})
    .then(function() {
        check_too_big(MAX_STREAMS_FOR_READ);
        var streams = _.keys(queries);
        logger.info('reading from', streams.length, 'streams');
        metrics.count('orestes.streams.op__read', streams.length, SAMPLE_RATE);
        return streams.map(function(stream) {
            var state;
            queries[stream] = _.sortBy(queries[stream], 'week');

            return function fetch(n, stats) {
                stats.requests += 1;
                var info = queries[stream][0];
                return cassUtils.execute_query(info.q, {
                    fetchSize: n,
                    pageState: state
                })
                .then(function(data) {
                    state = info.q;
                    stats.points += data.rows.length;

                    if (!data.more) {
                        queries[stream].shift();
                        state = null;
                    }

                    var pts = uncompactRows(data.rows, info.week, info.tags);

                    return {
                        points: pts,
                        space: info.space,
                        eof: queries[stream].length === 0
                    };
                })
                .catch(function(err) {
                    throw cass_errors.categorize_error(err, 'read');
                });
            };
        });
    });
}

// for each day between from and to, find all the streams we need to read from
// for that day and return an object with the right prepared queries
function buildQueries(docs, fromMs, toMs, queries) {
    var fromDay = Math.floor(fromMs / msInDay);
    var toDay = Math.floor(toMs / msInDay);
    var firstWeek = utils.roundToGranularity(fromDay);
    var lastWeek = utils.roundToGranularity(toDay);
    // for each metadatum, construct a query to Cassandra for each attrs
    // that that metadatum could define a tag key for
    return Promise.each(docs, function(doc) {
        var week = utils.dayFromIndex(doc._index);
        var fromOffset = week === firstWeek ? fromMs % msInWeek : 0;
        var toOffset = week === lastWeek ? toMs % msInWeek : msInWeek;
        var stream = doc._id;
        var space = utils.spaceFromIndex(doc._index);

        var key = 'space=' + space + ',' + stream;
        queries[key] = queries[key] || [];

        return utils.getPrepared(space, week, 'select')
            .then(function(prepared) {
                var q = prepared.query();
                q.bind([stream, fromOffset, toOffset], {});
                queries[key].push({
                    q: q,
                    week: week,
                    tags: doc._source,
                    space: space
                });
            });
    });
}

function _get_days(from, to) {
    var toMs = to ? to.milliseconds() : Date.now();
    var toDay = Math.floor(toMs / msInDay);
    if (toMs % msInWeek === 0) {
        // if the query end is right on a week boundary, then we don't need
        // metadata for that week
        toDay -= 1;
    }
    var fromMs = from ? from.milliseconds() : 0;
    if (fromMs === -Infinity) {
        return ['*'];
    }
    // the index where metadata for the specified start day would be stored
    var fromDay = utils.roundToGranularity(Math.floor(fromMs / msInDay));

    return _.range(fromDay, toDay+1, METADATA_GRANULARITY);
}

function _get_recent() {
    var now = Date.now();
    var the_week = msInWeek * Math.floor(now / msInWeek);
    var offset = now - the_week;
    if (offset < RECENT_SLOP) {
        the_week -= msInWeek;
    }

    return [ the_week/msInDay ];
}

function get_streams_query_size(spaces, options) {
    // in a scroll query, we get fetch_size documents from every shard
    // so if we're not using -recent then start by getting the number of
    // indices.  then we can calculate the total number of shards to
    // derive an appropriate per-shard fetch size.  we calculate the number
    // of shards using the formula: #shards <= cluster_size * #indexes
    var indices_promise;
    if (options.recent) {
        indices_promise = Promise.resolve(1);
    }
    else {
        var aliases_url = es_query.build_orestes_url(spaces, null, ['*'], null, '/_aliases');
        indices_promise = es_query.execute(aliases_url, null, 'GET')
        .then(function(indices) {
            return _.size(JSON.parse(indices));
        });
    }

    return indices_promise.then(function(num_indices) {
        var num_shards = num_indices * CLUSTER_SIZE;
        var size = options.streams_fetch_size || METADATA_FETCH_SIZE;
        if (num_shards === 0) {
            return 0;
        }

        return Math.ceil(size / num_shards);
    });
}

function stream_metadata(emit, es_filter, spaces, schemas, options) {
    if (options.metrics) {
        options.metrics.increment('elasticsearch.request.op__search_metadata', SAMPLE_RATE);
    }
    options = options || {};

    var cancelled = false;
    return get_streams_query_size(spaces, options)
    .then(function(size) {
        if (size === 0) {
            // no indices, nothing to read
            return {};
        }

        var params = {
            search_type: 'scan',
            scroll: '1m'
        };

        var days = options.recent ? _get_recent() : _get_days(options.from, options.to);
        var url = es_query.build_orestes_url(spaces, schemas, days, params);
        var body = {
            size: size
        };
        if (es_filter) {
            body.query = {
                filtered: {
                    filter: es_filter
                }
            };
        }

        logger.info('streams query body', JSON.stringify(body, null, 2));

        return es_query.execute(url, body, 'GET');
    })
    .then(function(response) {
        var scroll_url = es_query.build_scroll_url();
        function loop(res) {
            var result, body;
            return request.async({
                url: scroll_url,
                body: res._scroll_id
            })
            .spread(function(scrollRes, responseBody) {
                body = JSON.parse(responseBody);
                if (body.hits.total > options.stream_limit) {
                    if (options.metrics) {
                        options.metrics.increment('big_data_too_big.type__stream_metadata');
                    }
                    throw new errors.bigDataTooBig(null, null, {max: options.stream_limit, received: body.hits.total});
                }
                if (scrollRes.statusCode !== 200) {
                    var failure = body && body._shards && body._shards.failures && body._shards.failures[0];
                    if (failure) {
                        var error = es_errors.categorize_error(failure.reason);
                        if (error instanceof es_errors.ContextMissing) {
                            throw new errors.streamsTimeout();
                        }
                    }

                    logger.error(body);

                    throw new errors.internal();
                }
                result = body.hits.hits;

                if (result.length > 0) {
                    return emit(result);
                }
            })
            .then(function() {
                if (!cancelled && result.length !== 0 && body._scroll_id) {
                    return loop(body);
                }
            });
        }

        // if there's no data, you don't get a scroll_id, so just return
        if (response._scroll_id) {
            return loop(response);
        }
    })
    .cancellable()
    .catch(Promise.CancellationError, function() {
        cancelled = true;
    });
}

// get all the stream records that match the given filter/space/schemas
// emit() is a callable that is called with successive batches of records.
// this function returns a Promise that is resolved after all records
// have been emitted.
function get_stream_list(emit, es_filter, space, schemas, options) {
    var streams_bubo = options.recent ? null : new Bubo(utils.buboOptions);
    var space_bucket = space + '@1';

    function process(docs) {
        var out = docs;

        if (streams_bubo) {
            out = [];
            docs.forEach(function(doc) {
                // we're eliminating duplicates across buckets,
                // so always give Bubo the same bucket
                streams_bubo.lookup_point(space_bucket, doc._source, buboResult);
                if (!buboResult.found) {
                    out.push(doc);
                }
            });
        }

        out.forEach(function(hit) {
            hit._source.source_type = sourceTypeFromESDoc(hit._type);
        });

        emit(_.pluck(out, '_source'));
    }

    return stream_metadata(process, es_filter, space, schemas, options);
}

// optimized version of get_stream_list.
// unlike get_stream_list, can operate over multiple spaces at a time
function get_stream_list_opt(emit, es_filter, spaces, schemas, aggregations, options) {
    var days = options.recent ? _get_recent() : _get_days();
    var url = es_query.build_orestes_url(spaces, schemas, days, {});
    var body = {
        size: 0,
        aggregations: aggregations.es_aggr
    };
    if (es_filter) {
        body.query = {
            filtered: {
                filter: es_filter
            }
        };
    }

    logger.info('streams query body', JSON.stringify(body, null, 2));
    return es_query.execute(url, body, 'GET')
    .then(function(response) {
        var points = aggregation.values_from_es_aggr_resp(response, aggregations);
        emit(points);
    })
    .catch(es_errors.MissingField, function(miss) {
        aggregations = aggregation.remove_field(aggregations, miss.name);
        return get_stream_list_opt(emit, es_filter, spaces, schemas, aggregations, options);
    });
}

module.exports = {
    init: init,
    count_points: count_points,
    get_read_stats: get_read_stats,
    get_metric_streams: get_metric_streams,
    get_stream_list: get_stream_list,
    get_stream_list_opt: get_stream_list_opt
};
