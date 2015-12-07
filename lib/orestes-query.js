var _ = require('underscore');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
var utils = require('./orestes-utils');
var cassUtils = require('./cassandra').utils;
var es_query = require('./elasticsearch/query');
var es_errors = require('./elasticsearch/es-errors');
var cass_errors = require('./cassandra').errors;
var uncompactRows = utils.uncompactRows;
var aggregation = require('./elasticsearch/aggregation');
var Bubo = require('bubo');
var electra_utils = require('./elasticsearch/utils');

var logger = require('../logger').get('orestes');
var msInDay = 1000 * 60 * 60 * 24;

var es_url, METADATA_FETCH_SIZE_FOR_COUNT;
var METADATA_FETCH_SIZE, CONCURRENT_COUNTS;

var space_info;

// 30 minutes, make this configurable though
var RECENT_SLOP = 30 * 60 * 1000;

function init(config) {
    es_url = 'http://' + config.elasticsearch.host + ':' + config.elasticsearch.port + '/';
    space_info = config.spaces;
    METADATA_FETCH_SIZE = config.metadata_fetch_size || 20000;
    CONCURRENT_COUNTS = config.max_concurrent_count_requests || 20;
    METADATA_FETCH_SIZE_FOR_COUNT = config.metadata_fetch_size_for_count || 2000;
}

// find the indices that exist in this space
// used to keep us from trying to read nonexistent tables
// if your query's time range is longer than the span you have data
function get_valid_days(space) {
    return electra_utils.listIndices(es_url)
        .then(function(indices) {
            var days = Object.keys(indices).filter(function(index) {
                return utils.spaceFromIndex(index) === space;
            })
            .map(utils.dayFromIndex);

            return _.object(days, days);
        });
}

function count_points(es_filter, space, startMs, endMs, process_series) {
    var validDays;
    var bubo = new Bubo(utils.buboOptions);
    var space_bucket = space + '@1';

    var granularity = space_info[space].table_granularity_days;
    var msInWeek = granularity * msInDay;
    var startDay = Math.floor(startMs / msInDay);
    var endDay = Math.floor(endMs / msInDay);
    var firstWeek = utils.roundToGranularity(startDay, space);
    var lastWeek = utils.roundToGranularity(endDay, space);

    function count_doc_callback(docs) {
        return Promise.map(docs, function count_points_if_new_stream(doc) {
            var found = bubo.add(space_bucket, doc._source);
            if (found) {
                return;
            }

            return buildFetcher(doc, space, 'count', startMs, endMs, validDays)
                .then(function(fetcher) {
                    return process_series(fetcher);
                });

        }, {concurrency: CONCURRENT_COUNTS});
    }


    return get_valid_days(space)
        .then(function(valid_days) {
            validDays = valid_days;
            return stream_metadata(es_filter, space, startMs, endMs, count_doc_callback);
        });
}

// for each day between startMs and endMs in validDays, find all the streams we need
// to read from for that day and return an object with the right prepared queries
function buildFetcher(doc, space, queryType, startMs, endMs, validDays) {
    var stream = doc._id;
    var startDay = Math.floor(startMs / msInDay);
    var endDay = Math.floor(endMs / msInDay);
    var firstWeek = utils.roundToGranularity(startDay, space);
    var lastWeek = utils.roundToGranularity(endDay, space);
    var granularity = space_info[space].table_granularity_days;
    var allWeeks = _.range(firstWeek, lastWeek+1, granularity).filter(function(day) {
        return validDays.hasOwnProperty(day);
    });

    var msInWeek = granularity * msInDay;

    return Promise.map(allWeeks, function buildQuery(week) {
        var startOffset = week === firstWeek ? (startMs % msInWeek) : 0;
        var endOffset = week === lastWeek ? (endMs % msInWeek) : msInWeek;

        var key = 'space=' + space + ',' + stream;

        return utils.getPrepared(space, week, queryType)
            .then(function(prepared) {
                var q = prepared.query();
                q.bind([stream, startOffset, endOffset], {});
                return {
                    prepared: q,
                    week: week
                };
            });
    })
    .then(function(queries) {
        var state;

        return {
            tags: doc._source,
            fetch: function fetch(n) {
                var info = queries[0];
                var query_options = (n === -1) ? {autoPage: true} : {
                    fetchSize: n,
                    pageState: state
                };

                return cassUtils.execute_query(info.prepared, query_options)
                .then(function(data) {
                    state = info.prepared;

                    if (!data.more) {
                        queries.shift();
                        state = null;
                    }

                    var received_points = data.rows.map(function build_orestes_point_output(row) {
                        return row.hasOwnProperty('count') ? row.count : [info.week * msInDay + row.offset, row.value];
                    });

                    return {
                        points: received_points,
                        eof: queries.length === 0
                    };
                })
                .catch(function(err) {
                    throw cass_errors.categorize_error(err);
                });
            }
        };
    });
}

function _get_days(space, startMs, endMs) {
    var endDay = Math.floor(endMs / msInDay);
    var granularity = space_info[space].table_granularity_days;
    var msInWeek = granularity * msInDay;
    if (endMs % msInWeek === 0) {
        // if the query end is right on a week boundary, then we don't need
        // metadata for that week
        endDay -= 1;
    }

    var startDay = utils.roundToGranularity(Math.floor(startMs / msInDay), space);

    return _.range(startDay, endDay+1, space_info[space].table_granularity_days);
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

function stream_metadata(es_filter, space, startMs, endMs, es_document_callback) {
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

        var days = _get_days(space, startMs, endMs);
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

        logger.debug('executing', url, JSON.stringify(body, null, 2));

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
// process_streams() is a callable that is called with successive batches of records.
// this function returns a Promise that is resolved after all records
// have been emitted.
function get_stream_list(es_filter, space, startMs, endMs, process_streams) {
    var days = _get_days(space, startMs, endMs);
    var streams_bubo = days.length > 1 ? new Bubo(utils.buboOptions) : null;
    var space_bucket = space + '@1';

    function series_es_document_callback(docs) {
        var out = docs;

        if (streams_bubo) {
            out = [];
            docs.forEach(function(doc) {
                // we're eliminating duplicates across buckets,
                // so always give Bubo the same bucket
                var found = streams_bubo.add(space_bucket, doc._source);
                if (!found) {
                    out.push(doc._source);
                }
            });
        }

        return process_streams(out);
    }

    return stream_metadata(es_filter, space, startMs, endMs, series_es_document_callback);
}

function get_stream_list_opt(es_filter, space, aggregations) {
    var url = es_query.build_orestes_url(space, ['*'], {});
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

    logger.debug('streams query body', JSON.stringify(body, null, 2));
    return es_query.execute(url, body, 'GET')
    .then(function(response) {
        var points = aggregation.values_from_es_aggr_resp(response, aggregations);
        return points;
    })
    .catch(es_errors.ScriptMissing, function() {
        var err = new Error('You need to install scripts/aggkey.groovy to run multi-field select_distinct queries');
        err.status = 400;
        throw err;
    })
    .catch(es_errors.MissingField, function(miss) {
        aggregations = aggregation.remove_field(aggregations, miss.name);
        return get_stream_list_opt(es_filter, space, aggregations);
    });
}

function read(es_filter, space, startMs, endMs, options, process_series) {
    var bubo = new Bubo(utils.buboOptions);
    var space_bucket = space + '@1';
    var validDays;
    var total_series = 0;

    function es_document_callback(docs) {
        return Promise.map(docs, function read_points_if_new_stream(doc) {
            var found = bubo.add(space_bucket, doc._source);
            if (!found) {
                return buildFetcher(doc, space, 'select', startMs, endMs, validDays)
                    .then(function(fetcher) {
                        total_series++;
                        if (total_series > options.series_limit) {
                            throw new Error('query matched more than ' + options.series_limit + ' series');
                        }
                        return process_series(fetcher);
                    });
            }
        }, {concurrency: space_info[space].read_request_concurrency || 200});
    }

    return get_valid_days(space)
        .then(function(valid_days) {
            validDays = valid_days;

            return stream_metadata(es_filter, space, startMs, endMs, es_document_callback);
        });
}

function select_distinct(es_filter, space, keys) {
    var MAX_REDUCE_BY_SIZE = 1000000;
    var single_bucket_aggr = {
        group: {
            terms: {
                field: keys[0],
                size: 1000000
            }
        }
    };

    var multi_bucket_aggr = {
        group: {
            terms: {
                script_file: 'aggkey',
                lang: 'groovy',
                params: {
                    fields: keys
                },
                size: 1000000
            }
        }
    };

    var es_aggr = keys.length === 1 ? single_bucket_aggr : multi_bucket_aggr;
    var aggr_object = {
        es_aggr: es_aggr,
        empty_result: {},
        empty_fields :[],
        grouping: keys
    };

    return get_stream_list_opt(es_filter, space, aggr_object);
}

module.exports = {
    init: init,
    read: read,
    count_points: count_points,
    get_stream_list: get_stream_list,
    select_distinct: select_distinct
};
