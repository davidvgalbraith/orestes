var Promise = require('bluebird');
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var orestes_utils = require('../src/orestes-utils');
var test_utils = require('./orestes-test-utils');

/* Assumes Orestes/Cassie/ES already running, Orestes at localhost:9668 */

var BASE_URL = 'http://localhost:9668/';
var ES_MATCH_ALL = {
    match_all: {}
};

var msInDay = 1000 * 60 * 60 * 24;

function seriesFromPoints(points) {
    var grouped = _.groupBy(points, function(pt) {
        return orestes_utils.getAttributeString(pt);
    });
    var result = [];
    _.each(grouped, function(points, rowKey) {
        result.push({
            tags: _.omit(points[0], 'time', 'value'),
            points: points.map(function(pt) {
                return [new Date(pt.time).getTime(), pt.value];
            })
        });
    });

    return result;
}

function write(points) {
    if (!Array.isArray(points)) { points = [points]; }
    var write_url = BASE_URL + 'write';
    return request.postAsync({
        url: write_url,
        json: points
    })
    .spread(function(res, body) {
        expect(res.statusCode).equal(200);
        return body;
    });
}

function read(query, start, end) {
    var read_url = BASE_URL + 'read';
    return request.postAsync({
        url: read_url,
        json : {
            query: query || ES_MATCH_ALL,
            start: start || 0,
            end: end || Date.now()
        }
    })
    .spread(function(res, body) {
        expect(res.statusCode).equal(200);
        return body;
    });
}

function read_series(query) {
    var series_url = BASE_URL + 'series';

    return request.postAsync({
        url: series_url,
        json: {
            query: query || ES_MATCH_ALL,
            start: 0,
            end: Date.now()
        }
    })
    .spread(function(res, body) {
        expect(res.statusCode).equal(200);
        return body;
    });
}

function select_distinct(keys, query) {
    var select_distinct_url = BASE_URL + 'select_distinct';

    return request.postAsync({
        url: select_distinct_url,
        json: {
            keys: keys,
            query: query || ES_MATCH_ALL
        }
    })
    .spread(function(res, body) {
        expect(res.statusCode).equal(200);
        return body;
    });
}

function remove(space) {
    var delete_url = BASE_URL + 'delete';
    return request.postAsync({
        url: delete_url,
        json: {
            space: space,
            keep_days: 0
        }
    })
    .spread(function(res, body) {
        expect(res.statusCode).equal(200);
    });
}

function verify_import(points, query, expected) {
    return retry(function() {
        return read(query)
            .then(function(result) {
                expect(result).deep.equal(seriesFromPoints(expected || points));
            });
    });
}

function write_read_delete_test(points, query, expected, space) {
    space = space || 'default';
    return write(points)
        .then(function() {
            return verify_import(points, query, expected);
        })
        .then(function() {
            return remove(space);
        });
}

describe('Orestes', function() {
    this.timeout(30000);

    before(function() {
        return remove('default');
    });

    describe('basic functionality', function() {
        it('writes and reads a single point', function() {
            var one_point = test_utils.generate_sample_data({count: 1});
            return write_read_delete_test(one_point);
        });

        it('writes and reads several points', function() {
            var points = test_utils.generate_sample_data({count: 10});
            return write_read_delete_test(points);
        });

        it('writes and reads from several series', function() {
            var points = test_utils.generate_sample_data({
                count: 100,
                tags: {
                    host: ['a', 'b', 'c'],
                    pop: ['d', 'e', 'f', 'g'],
                    bananas: ['one', 'two', 'three', 'four', 'five']
                }
            });

            return write_read_delete_test(points);
        });

        it('writes and reads points over several days', function() {
            var start = Date.now() - msInDay * 10;
            var points = test_utils.generate_sample_data({
                count: 100,
                start: start,
                interval: msInDay/10,
                tags: {
                    host: ['a', 'b', 'c'],
                }
            });

            return write_read_delete_test(points);
        });

        it('with a nontrivial filter', function() {
            var points = test_utils.generate_sample_data({
                count: 100,
                tags: {
                    host: ['a', 'b', 'c'],
                    pop: ['d', 'e', 'f', 'g'],
                    bananas: ['one', 'two', 'three', 'four', 'five']
                }
            });

            var expected = points.filter(function(pt) {
                return pt.host === 'a';
            });

            return write_read_delete_test(points, {
                term: {
                    host: 'a'
                }
            }, expected);
        });
    });

    describe('error handling', function() {
        it('fails to write points without time', function() {
            var no_time = {value: 1, name: 'dave'};
            return write(no_time)
                .then(function(response) {
                    expect(response.errors).deep.equal([{
                        point: no_time,
                        error: 'missing required keys: ["time"]'
                    }]);
                });
        });

        it('fails to write points without value', function() {
            var no_value = {time: 1, name: 'dave'};
            return write(no_value)
                .then(function(response) {
                    expect(response.errors).deep.equal([{
                        point: no_value,
                        error: 'missing required keys: ["value"]'
                    }]);
                });
        });

        it('fails to write points with no tags', function() {
            var no_tags = {time: 1, value: 1};
            return write(no_tags)
                .then(function(response) {
                    expect(response.errors).deep.equal([{
                        point: no_tags,
                        error: 'metrics must have at least one tag'
                    }]);
                });
        });
    });

    describe('metadata queries', function() {
        function buildAttrString(d) {
            var keys = _.keys(d).sort();
            var strs = _.map(keys, function(key) {
                return key + '=' + d[key];
            });
            return strs.join(',');
        }

        function get_streams(data) {
            return _.chain(data)
                .sortBy(buildAttrString)
                .uniq(buildAttrString, true)
                .value();
        }

        var points = test_utils.generate_sample_data({
            count: 1000,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                bananas: ['one', 'two', 'three', 'four', 'five']
            }
        });

        before(function() {
            return write(points)
                .then(function() {
                    return verify_import(points);
                });
        });

        after(function() {
            return remove('default');
        });

        it('/series', function() {
            return read_series()
                .then(function(result) {
                    var received = _.sortBy(result, buildAttrString);
                    var expected = get_streams(points.map(function(pt) {
                        return _.omit(pt, 'time', 'value');
                    }));

                    expect(received).deep.equal(expected);
                });
        });

        it('/select_distinct host', function() {
            return select_distinct(['host'])
                .then(function(result) {
                    var expected = get_streams(points.map(function(pt) {
                        return {host: pt.host};
                    }));
                    var received = _.sortBy(result, 'host');

                    expect(received).deep.equal(expected);
                });
        });

        it('/select_distinct host, pop', function() {
            return select_distinct(['host', 'pop'])
                .then(function(result) {
                    var expected = get_streams(points.map(function(pt) {
                        return {host: pt.host, pop: pt.pop};
                    }));
                    var received = _.sortBy(result, buildAttrString);

                    expect(received).deep.equal(expected);
                });
        });
    });
});
