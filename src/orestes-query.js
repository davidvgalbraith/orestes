var _ = require('underscore');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
var Long = require('long');
var utils = require('./orestes-utils');
var cassUtils = require('./cassandra').utils;
var es_query = require('./elasticsearch/query');
var es_errors = require('./elasticsearch/es-errors');
var cass_errors = require('./cassandra').errors;
var uncompactRows = utils.uncompactRows;
var aggregation = require('./elasticsearch/aggregation');
var Bubo = require('./bubo');
var sourceTypeFromESDoc = require('./elasticsearch/utils').sourceTypeFromESDoc;

var logger = require('logger').get('orestes');
var msInDay = 1000 * 60 * 60 * 24;

var es_url, METADATA_FETCH_SIZE_FOR_COUNT;
var METADATA_FETCH_SIZE, CONCURRENT_COUNTS;

var space_info;

// 30 minutes, make this configurable though
var RECENT_SLOP = 30 * 60 * 1000;

var buboResult = {};


function init(config) {
    es_url = 'http://' + config.elasticsearch.host + ':' + config.elasticsearch.port + '/';
    space_info = config.spaces;
    METADATA_FETCH_SIZE = config.metadata_fetch_size || 20000;
    CONCURRENT_COUNTS = config.max_concurrent_count_requests || 20;
    METADATA_FETCH_SIZE_FOR_COUNT = config.metadata_fetch_size_for_count || 2000;
}

function count_points(es_filter, space, from, to, groupby, emit) {
    var granularity = space_info[space].table_granularity_day;
    var msInWeek = granularity * msInDyay;
    var fromMs = from ? from.milliseconds() : 0;
    var toMs = to ? to.milliseconds() : Date.now();
    var fromDay = Math.floor(fromMs / msInDay);
    var toDay = Math.floor(toMs / msInDay);
    var firstWeek = utils.roundToGranularity(fromDay);
    var lastWeek = utils.roundToGranularity(toDay);
    var options = {
        from: from,
        to: to,
        streams_fetch_size: METADATA_FETCH_SIZE_FOR_COUNT,
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
    return stream_metadata(add_counts, es_filter, spaces, options);
}

function get_metric_streams(es_filter, space, from, to) {
    throw "this is broken now";
    var queries = {};
    var fromMs = from.milliseconds();
    var toMs = to ? to.milliseconds() : Date.now();
    function build_queries(docs) {
        return buildQueries(docs, space, fromMs, toMs);
    }
    return stream_metadata(build_queries, es_filter, spaces, {from: from, to: to})
    .then(function() {
        var streams = _.keys(queries);
        logger.info('reading from', streams.length, 'streams');
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
                    throw cass_errors.categorize_error(err);
                });
            };
        });
    });
}

// for each day between from and to, find all the streams we need to read from
// for that day and return an object with the right prepared queries
function buildQueries(doc, space, fromMs, toMs) {
    var stream = doc._id;
    var fromDay = Math.floor(fromMs / msInDay);
    var toDay = Math.floor(toMs / msInDay);
    var firstWeek = utils.roundToGranularity(fromDay, space);
    var lastWeek = utils.roundToGranularity(toDay, space);
    var granularity = space_info[space].table_granularity_days;
    var allWeeks = _.range(firstWeek, lastWeek, granularity);
    var msInWeek = granularity * msInDay;

    return Promise.map(allWeeks, function buildQuery(week) {
        var fromOffset = week === firstWeek ? (fromMs % msInWeek) : 0;
        var toOffset = week === lastWeek ? (toMs % msInWeek) : msInWeek;

        var key = 'space=' + space + ',' + stream;

        return utils.getPrepared(space, week, 'select')
            .then(function(prepared) {
                var q = prepared.query();
                q.bind([stream, fromOffset, toOffset], {});
                return {
                    prepared: q,
                    week: week
                };
            });
    })
    .then(function(queries) {
        return {
            queries: queries,
            tags: doc._source
        };
    });
}

function _get_days(space, fromMs, toMs) {
    var toDay = Math.floor(toMs / msInDay);
    var granularity = space_info[space].table_granularity_days;
    var msInWeek = granularity * msInDay;
    if (toMs % msInWeek === 0) {
        // if the query end is right on a week boundary, then we don't need
        // metadata for that week
        toDay -= 1;
    }

    // the index where metadata for the specified start day would be stored
    var fromDay = utils.roundToGranularity(Math.floor(fromMs / msInDay));

    return _.range(fromDay, toDay+1, space_info[space].table_granularity_days);
}

function _get_recent(space) {
    var granularity = space_info[space].table_granularity_days;
    var msInWeek = msInDay * granularity;
    var now = Date.now();
    var the_week = msInWeek * Math.floor(now / msInWeek);
    var offset = now - the_week;
    if (offset < RECENT_SLOP) {
        the_week -= msInWeek;
    }

    return [ the_week/msInDay ];
}

function get_streams_query_size(space) {
    // in a scroll query, we get fetch_size documents from every shard
    // so if we're not using -recent then start by getting the number of
    // indices.  then we can calculate the total number of shards to
    // derive an appropriate per-shard fetch size.  we could calculate the number
    // of shards using the formula: #shards <= cluster_size * #indexes
    // but Orestes 0.1 doesn't have the #shards number so we assume it's 1
    var aliases_url = es_query.build_orestes_url(space, null, ['*'], null, '/_aliases');
    return es_query.execute(aliases_url, null, 'GET')
    .then(function(indices) {
        var num_indices = _.size(JSON.parse(indices));
        if (num_indices === 0) {
            return 0;
        }

        return Math.ceil(METADATA_FETCH_SIZE / num_indices);
    });
}

function stream_metadata(es_filter, space, fromMs, toMs, es_document_callback) {
    var cancelled = false;

    return get_streams_query_size(space)
    .then(function(size) {
        if (size === 0) {
            // no indices, nothing to read
            return {};
        }

        var params = {
            search_type: 'scan',
            scroll: '10m'
        };

        var days = _get_days(space, fromMs, toMs);
        var url = es_query.build_orestes_url(space, days, params);
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

        logger.info('executing', url, JSON.stringify(body, null, 2));

        return es_query.execute(url, body, 'GET');
    })
    .then(function(response) {
        var scroll_url = es_query.build_scroll_url();
        function loop(res) {
            var result, body;
            return request.getAsync({
                url: scroll_url,
                body: res._scroll_id
            })
            .spread(function(scrollRes, responseBody) {
                body = JSON.parse(responseBody);
                if (scrollRes.statusCode !== 200) {
                    var failure = body && body._shards && body._shards.failures && body._shards.failures[0];
                    if (failure) {
                        var error = es_errors.categorize_error(failure.reason);
                        if (error instanceof es_errors.ContextMissing) {
                            throw new Error('Request timed out');
                        }
                    }

                    logger.error(body);

                    throw new Error('Elasticsearch scroll query failed: ' + JSON.stringify(responseBody));
                }
                result = body.hits.hits;

                if (result.length > 0) {
                    return es_document_callback(result);
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

// get all the stream records that match the given filter/space
// emit() is a callable that is called with successive batches of records.
// this function returns a Promise that is resolved after all records
// have been emitted.
function get_stream_list(emit, es_filter, space, options) {
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

    return stream_metadata(process, es_filter, space, options);
}

// optimized version of get_stream_list.
// unlike get_stream_list, can operate over multiple spaces at a time
function get_stream_list_opt(es_filter, space, aggregations, options, emit) {
    var days = options.recent ? _get_recent(space) : _get_days(space);
    var url = es_query.build_orestes_url(space, days, {});
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
        return get_stream_list_opt(emit, es_filter, spaces, aggregations, options);
    });
}

function read(es_filter, space, from, to, process_series) {
    var bubo = new Bubo(utils.buboOptions);
    var space_bucket = space + '@1';
    var fromMs = new Date(from).getTime();
    var toMs = new Date(to).getTime();

    function es_document_callback(docs) {
        return Promise.each(docs, function read_points_if_new_stream(doc) {
            bubo.lookup_point(space_bucket, doc._source, buboResult);
            if (!buboResult.found) {
                return buildQueries(doc, space, fromMs, toMs)
                    .then(function(query_object) {
                        var points = [];
                        return Promise.each(query_object.queries, function execute_query_and_buffer_results(query) {
                            return cassUtils.execute_query(query.prepared, {
                                autoPage: true
                            })
                            .then(function(result) {
                                var received_points = result.rows.map(function build_orestes_point_output(row) {
                                    return [query.week * msInDay + row.offset, row.value];
                                });

                                points = points.concat(received_points);
                            });
                        })
                        .then(function() {
                            return process_series({
                                tags: doc._source,
                                points: points
                            });
                        });
                    });
            }
        });
    }

    return stream_metadata(es_filter, space, fromMs, toMs, es_document_callback);
}

module.exports = {
    init: init,
    read: read,
    count_points: count_points,
    get_metric_streams: get_metric_streams,
    get_stream_list: get_stream_list,
    get_stream_list_opt: get_stream_list_opt
};
