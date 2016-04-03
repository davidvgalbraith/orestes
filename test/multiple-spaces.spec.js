var test_utils = require('./orestes-test-utils');

var msInDay = 1000 * 60 * 60 * 24;

describe('embedded Orestes API', function() {
    this.timeout(30000);

    before(function() {
        var config = {
            port: 9668,
            cassandra: {
                host: '127.0.0.1',
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

        return test_utils.start_orestes(config)
            .then(function() {
                return test_utils.clear_spaces(['default', 'other']);
            });
    });

    after(function() {
        return test_utils.stop_orestes().then(function() {
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
