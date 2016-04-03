var _ = require('underscore');
var errors = require('./es-errors');

var MAX_REDUCE_BY_SIZE = 1000000;

function make_bucket_agg(groupby, subagg) {
    var bucket_agg = {};
    groupby.forEach(function(field) {
        bucket_agg[field] = {
            terms: {
                field: field,
                size: MAX_REDUCE_BY_SIZE
            }
        };
    });

    return _.reduceRight(bucket_agg, function nest(memo, value, key) {
        var obj = {};

        if (!_.isEmpty(memo)) {
            value.aggregations = memo;
        }

        obj[key] = value;

        return obj;
    }, subagg);
}

// utility to help with the case when we group by a non-existent field.
// given a grouped aggregation `aggr`, remove the field `name` from the
// grouping but store it in a list of empty_fields.  we will insert a
// point with the value null for each of these empty fields in
// points_from_grouped_response() below.  see query.js for details of how
// this is used.
function remove_field(aggr, name) {
    var empty_fields = aggr.empty_fields.slice(0);
    empty_fields.push(name);
    var grouping = _.without(aggr.grouping, name);

    var new_es_aggr;
    if (grouping.length === 1) {
        new_es_aggr = {
            group: {
                terms: {
                    field: grouping[0],
                    size: aggr.es_aggr.group.terms.size
                }
            }
        };

        if (aggr.es_aggr.group.aggregations) {
            new_es_aggr.group.aggregations = _.clone(aggr.es_aggr.group.aggregations);
        }
    }
    else {
        new_es_aggr = _.clone(aggr.es_aggr);
        new_es_aggr.group.terms.params.fields = grouping;
    }

    return {
        es_aggr: new_es_aggr,
        empty_result: aggr.empty_result,
        grouping: grouping,
        count: aggr.count,
        empty_fields: empty_fields
    };
}

function _assert_nonempty_group(bucket, field) {
    if (!bucket.buckets.length) {
        throw new errors.MissingField(field);
    }
}

function _aggregation_values(aggregation_result, count, info) {
    var pt = _.clone(info.empty_result);
    var aggr_names = info.aggr_names || [];
    aggr_names.forEach(function(op) {
        var key = op[0];
        var reducer = op[1];
        if (_.has(aggregation_result, key)) {
            if (reducer === 'stdev') {
                pt[key] = aggregation_result[key].std_deviation;
            } else {
                pt[key] = aggregation_result[key].value;
            }
        }
    });

    if (info.count) {
        pt[info.count] = count;
    }

    info.empty_fields.forEach(function(field) {
        pt[field] = null;
    });

    return pt;
}

function _time_sort(points) {
    if (_.has(points[0], 'time')) {
        return _.sortBy(points, 'time');
    }

    return points;
}

function points_from_grouped_response(response, info) {
    var points = [];
    function points_from_group(aggregation, fields, point_base) {
        if (!aggregation) { return; }
        var field = fields.shift();
        var bucket = aggregation[field];
        _assert_nonempty_group(bucket, field);
        if (fields.length === 0) {
            var new_points = bucket.buckets.map(function base_case(b) {
                var pt = _aggregation_values(b, b.doc_count, info);
                pt[field] = b.key_as_string || b.key;

                // Following Juttle 'reduce' behavior, we don't output points
                // for an empty batch if the reducer has 'groupby'
                var should_return_point = !(info.grouping.length && !b.doc_count);

                return should_return_point && _.extend(pt, point_base);
            });

            points = points.concat(_.compact(new_points));
        } else {
            bucket.buckets.forEach(function extend_base_and_recurse(b) {
                var base = _.clone(point_base);
                base[field] = b.key;
                points_from_group(b, _.clone(fields), base);
            });
        }
    }

    var grouping = _.clone(info.grouping);

    points_from_group(response.aggregations, grouping, {});

    return _time_sort(points);
}

module.exports = {
    make_bucket_agg: make_bucket_agg,
    remove_field: remove_field,
    points_from_grouped_response: points_from_grouped_response
};
