var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
var _ = require('underscore');
var util = require('util');
var minimist = require('minimist');

var options = minimist(process.argv.slice(2));
var NUM_POINTS = options.num_points || 100000;
var WRITE_BATCH_SIZE = options.write_batch_size || 500;
var NUM_WRITES = NUM_POINTS / WRITE_BATCH_SIZE;

var NUM_TAGS = options.num_tags || 3;
var VALS_PER_TAG = options.num_values || 10;

function randInt(max) {
    return Math.floor(Math.random() * max);
}

var point_time = Date.now() - NUM_POINTS;

function generate_data() {
    var data = [];
    for (var i = 0; i < WRITE_BATCH_SIZE; i++) {
        var point = {
            time: point_time++,
            value: randInt(1000)
        };

        for (var j = 0; j < NUM_TAGS; j++) {
            point['tag' + j] = 'value' + randInt(VALS_PER_TAG);
        }

        data.push(point);
    }

    return data;
}

var read_start;
var write_start;

return request.postAsync({
    url: 'http://localhost:9668/delete',
    json: {
        space: 'default',
        keep_days: 0
    }
})
.then(function() {
    write_start = Date.now();
    console.log('writing %d points', NUM_POINTS);
    return Promise.map(_.range(NUM_WRITES), function(n) {
        return request.postAsync({
            url: 'http://localhost:9668/write',
            json: generate_data()
        });
    }, {concurrency: 10});
})
.then(function() {
    var elapsed = Date.now() - write_start;
    console.log(util.format('wrote %d points in %d seconds', NUM_POINTS, elapsed / 1000));
    console.log('preparing to read...');
    return Promise.delay(3000);
})
.then(function() {
    console.log('reading');
    read_start = Date.now();
    return request.postAsync({
        url: 'http://localhost:9668/read',
        json: {
            start: 0,
            end: Date.now(),
            query: {
                match_all: {}
            }
        }
    });
})
.spread(function(res, body) {
    var elapsed = Date.now() - read_start;
    var num_series = body.series.length;
    var read_points = _.reduce(body.series, function(memo, series) {
        return memo + series.points.length;
    }, 0);

    if (read_points !== NUM_POINTS) {
        console.error(util.format('fraud detected -- expected %d points but read %d', NUM_POINTS, read_points));
    } else {
        console.log(util.format('read %d points from %d series in %d seconds', read_points, num_series, elapsed / 1000));
    }
});
