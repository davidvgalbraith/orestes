var Promise = require('bluebird');
var retry = require('bluebird-retry');
var _ = require('underscore');
var expect = require('chai').expect;

var test_utils = require('./orestes-test-utils');
var sort_series = test_utils.sort_series;
var series_from_points = test_utils.series_from_points;
var Orestes = require('../src/orestes');

var ES_MATCH_ALL = {
    match_all: {}
};

var msInDay = 1000 * 60 * 60 * 24;

describe('embedded Orestes API', function() {
    this.timeout(30000);

    before(function() {
        var config = {
            port: 9668,
            cassandra: {
                host: '127.94.0.1',
                native_transport_port: 9042
            },
            elasticsearch: {
                host: 'localhost',
                port: 9200
            },
            spaces: {
                default: {
                    table_granularity_days: 1
                },
                other: {
                    table_granularity_days: 7
                }
            }
        };

        return Orestes.startup(config)
            .then(function() {
                return test_utils.clear_spaces(['default', 'other']);
            });
    });

    describe('another space', function() {
        var default_points = test_utils.generate_sample_data({
            start: Date.now() - 1000,
            count: 1000,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                bananas: ['one', 'two', 'three', 'four', 'five']
            }
        });

        var other_points = test_utils.generate_sample_data({
            start: Date.now() - 100 * msInDay,
            interval: msInDay/10,
            count: 1000,
            tags: {
                host: ['h', 'i', 'j'],
                pop: ['k', 'l', 'm', 'n'],
                bananas: ['six', 'seven', 'eight', 'nine', 'ten']
            }
        });

        before(function() {
            return test_utils.write(default_points)
                .then(function() {
                    return test_utils.verify_import(default_points);
                });
        });

        it('reads and writes points to another space', function() {
            return test_utils.write(other_points, 'other')
                .then(function() {
                    return test_utils.verify_import(other_points, 'other');
                });
        });
    });
});
