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

describe('embedded Orestes API', function() {
    this.timeout(30000);

    before(function() {
        return test_utils.start_orestes();
    });

    after(function() {
        return test_utils.stop_orestes();
    });

    describe('read', function() {
        var points = test_utils.generate_sample_data({
            start: Date.now() - 1000,
            count: 1000,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                bananas: ['one', 'two', 'three', 'four', 'five']
            }
        });

        var expected = sort_series(series_from_points(points));

        before(function() {
            return test_utils.write(points)
                .then(function() {
                    return test_utils.verify_import(points);
                });
        });

        after(function() {
            return test_utils.remove('default');
        });

        it('embedded API', function() {
            var all_fetchers = [];
            function process_series(fetcher) {
                all_fetchers.push(fetcher);
            }

            return Orestes.read(ES_MATCH_ALL, 'default', 0, Date.now(), {}, process_series)
            .then(function() {
                expect(all_fetchers.length).equal(expected.length);
                return Promise.map(all_fetchers, function loop(fetcher) {
                    fetcher.points = fetcher.points || [];
                    return fetcher.fetch(2)
                        .then(function(result) {
                            expect(result.points.length <= 2).equal(true);
                            fetcher.points = fetcher.points.concat(result.points);
                            if (!result.eof) {
                                return loop(fetcher);
                            }
                        });
                });
            })
            .then(function() {
                var received = sort_series(all_fetchers.map(function(fetcher) {
                    return {tags: fetcher.tags, points: fetcher.points};
                }));

                expect(received).deep.equal(expected);
            });
        });
    });

    describe('write', function() {
        after(function() {
            return test_utils.remove('default');
        });

        it('embedded API', function() {
            var points = test_utils.generate_sample_data({
                start: Date.now() - 1000,
                count: 1000,
                tags: {
                    type: ['write_test'],
                    host: ['a', 'b', 'c'],
                    pop: ['d', 'e', 'f', 'g'],
                    bananas: ['one', 'two', 'three', 'four', 'five']
                }
            });

            return Orestes.write(points, 'default')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    return retry(function() {
                        return test_utils.verify_import(points);
                    });
                });
        });
    });

    describe('limits', function() {
        var points = test_utils.generate_sample_data({
            start: Date.now() - 1000,
            count: 1000,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                bananas: ['one', 'two', 'three', 'four', 'five']
            }
        });

        var expected = sort_series(series_from_points(points));

        before(function() {
            return test_utils.write(points)
                .then(function() {
                    return test_utils.verify_import(points);
                });
        });

        after(function() {
            return test_utils.remove('default');
        });

        it('errors if you read more than a specified series_limit', function() {
            var options = {
                series_limit: 10
            };
            return Orestes.read(ES_MATCH_ALL, 'default', 0, Date.now(), options, _.noop)
                .then(function() {
                    throw new Error('should have failed');
                })
                .catch(function(err) {
                    expect(err.message).equal('query matched more than 10 series');
                });
        });
    });
});
