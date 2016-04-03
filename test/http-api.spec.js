var _ = require('underscore');
var expect = require('chai').expect;
var oboe = require('oboe');

var test_utils = require('./orestes-test-utils');
var sort_series = test_utils.sort_series;
var write = test_utils.write;
var verify_import = test_utils.verify_import;
var select_distinct = test_utils.select_distinct;
var build_attr_string = test_utils.build_attr_string;

var msInDay = 1000 * 60 * 60 * 24;

describe('Orestes', function() {
    this.timeout(30000);

    before(function() {
        return test_utils.start_orestes();
    });

    after(function() {
        return test_utils.stop_orestes();
    });

    describe('basic functionality', function() {
        function write_read_delete_test(points, query, expected, space) {
            space = space || 'default';
            return write(points, space)
                .then(function() {
                    return verify_import(points, space, query, expected);
                })
                .then(function() {
                    return test_utils.remove(space);
                });
        }

        before(function() {
            return test_utils.remove('default');
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

    describe('count', function() {
        function counts_from_points(points) {
            var serieses = test_utils.series_from_points(points);
            serieses.forEach(function(series) {
                series.count = series.points.length;
                delete series.points;
            });

            return serieses;
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
            return test_utils.remove('default');
        });

        it('all points', function() {
            return test_utils.count()
                .then(function(result) {
                    var received = sort_series(result.series);
                    var expected = sort_series(counts_from_points(points));

                    expect(received).deep.equal(expected);
                });
        });

        it('with filter', function() {
            return test_utils.count({
                term: {
                    host: 'a'
                }
            })
            .then(function(result) {
                var expected = sort_series(counts_from_points(points.filter(function(pt) {
                    return pt.host === 'a';
                })));

                var received = sort_series(result.series);

                expect(received).deep.equal(expected);
            });
        });

        it('over several days', function() {
            var start = Date.now() - msInDay * 10;

            var points = test_utils.generate_sample_data({
                count: 100,
                start: start,
                interval: msInDay/10,
                tags: {
                    host: ['a', 'b', 'c'],
                    name: ['several_days']
                }
            });

            var name_filter = {
                term: {
                    name: 'several_days'
                }
            };

            return write(points)
                .then(function() {
                    return verify_import(points, 'default', name_filter);
                })
                .then(function() {
                    return test_utils.count(name_filter);
                })
                .then(function(result) {
                    var expected = sort_series(counts_from_points(points));
                    var received = sort_series(result.series);

                    expect(received).deep.equal(expected);
                });
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
        function get_streams(data) {
            return _.chain(data)
                .sortBy(build_attr_string)
                .uniq(build_attr_string, true)
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
            return test_utils.remove('default');
        });

        it('/series', function() {
            return test_utils.read_series()
                .then(function(result) {
                    var received = _.sortBy(result.series, build_attr_string);
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
                    var received = _.sortBy(result, build_attr_string);

                    expect(received).deep.equal(expected);
                });
        });
    });

    describe('streaming', function() {
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
            return test_utils.remove('default');
        });

        it('streams series as soon as they\'re available', function(done) {
            var invocations = 0;
            oboe({
                method: 'POST',
                url: 'http://localhost:9668/read',
            })
            .node('series.*', function(series) {
                invocations++;
                expect(series.points).to.exist;
                expect(series.tags).to.exist;
            })
            .done(function(result) {
                try {
                    var expected = sort_series(test_utils.series_from_points(points));
                    expect(invocations).equal(expected.length);
                    expect(sort_series(result.series)).deep.equal(expected);
                    done();
                } catch (err) {
                    done(err);
                }
            });
        });
    });
});
