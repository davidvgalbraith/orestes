
var _ = require('underscore');

var Logger = require('../../logger');
var logger = Logger.get('elasticsearch');

// build a bucketed aggregation that groups by the given list of
// field names (and optionally includes given sub-aggregations).
// groupby is a list of field name and size is how many buckets to create
function make_bucket_agg(groupby) {
    var agg;

    // if we're grouping by a single field, we use a straightforward
    // terms aggregation.
    if (groupby.length === 1) {
        agg = {
            terms: { field: groupby[0] }
        };
    }

    // if we're grouping by multiple fields simultaneously,
    // use a scripted terms aggregation as suggested here:
    // http://www.elastic.co/guide/en/elasticsearch/reference/1.4/search-aggregations-bucket-terms-aggregation.html#_multi_field_terms_aggregation
    else {
        agg = {
            terms: {
                script_file: 'aggkey',
                lang: 'groovy',
                params: { fields: groupby }
            }
        };
    }

    agg.terms.size = 0;

    return { group: agg };
}

// as mentioned above in make_bucket_agg(), when we group by multiple
// fields at once, we use a script written in groovy to build a string
// representation of the values of all the grouping fields and then do
// a single flat aggregation on that string.  the script "aggkey"
// referenced above builds this string, it can be found under
// elasticsearch-kid/scripts/
// this string must encode both the value and the type of each field
// value.   the encoded representation of both strings and numbers begins
// with a number (a sequence of digits), followed by a letter indicating
// the type (s for string, n for number).  in the case of a string, the
// leading number is the length in characters of the string.  in the
// case of a number, the value is the leading number.
// for example, consider the following points being reduced by host and pid:
// [
//  { host: "A,B", pid: 123 },
//  { host: "A,B", pid: 456 },
//  { host: "C,D", pid: 789 }
// ]
// we need to build unique aggregation keys that include the strings
// "A,B" and "C,D" as well the numbers 123, 456, and 789.  the keys would
// look like this:
//  3sA,B123n  3sA,B456n  3sC,D789n
// these strings are not optimized for human readability but they can
// be created relatively efficiently and can be parsed unambiguously.
//
// also there can be 0z which means null
// i.e. if our example array also had {host: "Q"}
// then we'd get a key 1sQ,0z
//
// this routine (_parse_aggkey) does the opposite of the groovy script --
// it converts an aggregation key in the format described above into a
// list of values.

var PARSE_PATTERN = /^([0-9.]+)([snz])/;
function _parse_aggkey(s) {
    var ret = [];
    while (s.length > 0) {
        var m = s.match(PARSE_PATTERN);
        if (m) {
            var end;
            var N = parseInt(m[1], 10);

            switch (m[2]) {
            case 's':
                // N is string length...
                var from = m[0].length;
                var to = from + N;
                if (s.length < to) {
                    throw new Error('truncated string');
                }
                ret.push(s.substring(from, to));
                end = to;
                break;

            case 'n':
                ret.push(N);
                end = m[0].length;
                break;

            case 'z':
                ret.push(null);
                end = m[0].length;
                break;

            default:
                throw new Error('malformed agg key' +
                    ' (unexpected character '+ m[2] + ')');
            }

            s = s.substring(end);
        }
        else {
            throw new Error('malformed agg key (did not match PARSE_PATTERN)');
        }
    }
    return ret;
}

// utility to help with the case when we group by a non-existent field.
// given a grouped aggregation `aggr`, remove the field `name` from the
// grouping but store it in a list of empty_fields.  we will insert a
// point with the value null for each of these empty fields in
// values_from_es_aggr_resp() below.  see query.js for details of how
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

// The function below takes an ES response that includes aggregations
// and builds a corresponding Juttle result (i.e., an array of points).
function values_from_es_aggr_resp(response, info) {
    var proto = {};
    var aggr_names = info.aggr_names || [];
    if (info && info.empty_fields) {
        _.each(info.empty_fields, function(name) {
            proto[name] = null;
        });
    }

    var result = response.aggregations;

    function do_leaf(result, proto, grouped) {
        if (!result) {
            return [_.clone(info.empty_result)];
        }
        if (_.has(result, '_time')) {
            var pts = [];
            _.each(result._time.buckets, function(bucket) {
                // Following Juttle 'reduce' behavior, we don't output points
                // for an empty batch if the reducer has 'groupby'
                if (grouped && bucket.doc_count === 0) {
                    return;
                }

                _.extend(proto, { time: bucket.key_as_string });
                if (info.count) {
                    proto[info.count] = bucket.doc_count;
                }
                pts.push.apply(pts, do_leaf(bucket, proto));
            });

            return pts;
        }

        var pt = {};
        for (var protoKey in proto) {
            pt[protoKey] = proto[protoKey];
        }
        for (var k = 0; k < aggr_names.length; k++) {
            var key = aggr_names[k];
            if (result.hasOwnProperty(key)) {
                pt[key] = result[key].value;
            }
        }

        if (info.count) {
            proto[info.count] = response.hits.total;
        }

        return [ pt ];
    }

    if (info && info.grouping && info.grouping.length > 0) {
        if (!result) {
            return [];
        }

        var rv = [];

        _.each(result.group.buckets, function(bucket) {
            if (info.grouping.length === 1) {
                proto[info.grouping[0]] = bucket.key;
            }
            else {
                var L = _parse_aggkey(bucket.key);
                if (L.length !== info.grouping.length) {
                    var msg = 'malformed agg key' +
                        ' (expected ' + info.grouping.length +
                        ' fields, got ' + L.length + ')';
                    logger.error(msg);
                    throw new Error(msg);
                }
                proto = _.object(_.zip(info.grouping, L));
            }

            if (info.count) {
                proto[info.count] = bucket.doc_count;
            }

            // grab values from any sub-aggregations
            var pts = do_leaf(bucket, proto, true);
            rv.push.apply(rv, pts);
        });

        // These points should all be uniform.  So if we have a date
        // histogram anywhere, all the points should have a time
        // property and we put them all in order here.
        if (_.has(rv[0], 'time')) {
            rv = _.sortBy(rv, 'time');
        }
        return rv;
    }
    else {
        var pts = do_leaf(result, proto, false);
        if (info.count && !_.has(result, '_time')) {
            pts[0][info.count] = response.hits.total;
        }
        return pts;
    }
}

module.exports = {
    make_bucket_agg: make_bucket_agg,
    remove_field: remove_field,
    values_from_es_aggr_resp: values_from_es_aggr_resp
};
