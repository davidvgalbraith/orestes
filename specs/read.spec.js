var Promise = require('bluebird');
var _ = require('underscore');
var request = Promise.promisifyAll(require('request'));
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var orestes_utils = require('../src/orestes-utils');
var test_utils = require('./orestes-test-utils');

/* Assumes Orestes/Cassie/ES already running, Orestes at localhost:9668 */

var BASE_URL = 'http://localhost:9668/';

var points = [
    { time: '2015-11-08T04:00:00.000Z', host: 'host1', pop: 'sea', value: 1 },
    { time: '2015-11-08T05:00:00.000Z', host: 'host1', pop: 'sea', value: 2 },
    { time: '2015-11-08T04:00:00.000Z', host: 'host1', pop: 'sfo', value: 3 }
];

function seriesFromPoints(points) {
    var grouped = _.groupBy(points, function(pt) {
        return utils.getAttributeString(pt);
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
        console.error('reite me', JSON.stringify(body, null, 2))
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
    var delete_url = BASE_URL;
    return request.deleteAsync({
        url: delete_url,
        json: {
            space: space,
            keep_days: 0
        }
    })
    .spread(function(res, body) {
        console.error('beleep me', JSON.stringify(body, null, 2));
    });
}

function write_read_delete_test(points, space) {
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

    it('writes and reads a single point', function() {
        var one_point = test_utils.generate_sample_data({count: 1});
        return write_read_delete_test(one_point);
    });
});
