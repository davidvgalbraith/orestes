var Promise = require('bluebird');
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var orestes_utils = require('../src/orestes-utils');
var test_utils = require('./orestes-test-utils');

/* Assumes Orestes/Cassie/ES already running, Orestes at localhost:9668 */

var BASE_URL = 'http://localhost:9668/';
var msInDay = 1000 * 60 * 60 * 24;

var points = [
    { time: '2015-11-08T04:00:00.000Z', host: 'host1', pop: 'sea', value: 1 },
    { time: '2015-11-08T05:00:00.000Z', host: 'host1', pop: 'sea', value: 2 },
    { time: '2015-11-08T04:00:00.000Z', host: 'host1', pop: 'sfo', value: 3 }
];

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
    var write_url = BASE_URL + 'write';
    return request.postAsync({
        url: write_url,
        json: points
    })
    .spread(function(res, body) {
        expect(res.statusCode).equal(200);
    });
}

function read_all() {
    var read_url = BASE_URL + 'read';
    return request.postAsync({
        url: read_url,
        json : {
            query: {
                match_all: {}
            },
            start: 0,
            end: Date.now()
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

function write_read_delete_test(points, space) {
    space = space || 'default';
    return write(points)
        .then(function() {
            return retry(function() {
                return read_all()
                    .then(function(result) {
                        expect(result).deep.equal(seriesFromPoints(points));
                    });
            });
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
});
