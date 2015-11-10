var _ = require('underscore');

function randInt(max) {
    return Math.floor(Math.random() * max);
}

// generates sample data for tests
// info is an object describing the data you want
// Possible keys for info:
// count: tells how many points you want to create, defaults to 10
// start: the timestamp of the earliest point you want to import
// interval: the interval between timestamps, in milliseconds
// tags: an object of the form {tagName1: [tag1Value1, tag1Value2...], tagName2: [tag2Value1,tag2value2,...]}
//     will create an equal number of points having each value for each tag (+/- 1 for divisibility)
function generate_sample_data(info) {
    info = info || {};
    var sampleData = [];

    var count = info.count || 10;
    var tags = info.tags || {name: 'test'};
    var interval = info.interval || 1;
    var date = (info.start) ? new Date(info.start) : new Date();

    for (var k = 0; k < count; k++) {
        var pointTags = {};

        _.each(tags, function(values, key) {
            pointTags[key] = values[k % values.length];
        });

        var sampleMetric = {
            time: date.toISOString(),
            value: randInt(100)
        };

        sampleData.push(_.extend(sampleMetric, pointTags));

        date.setTime(date.getTime() + interval);
    }

    return sampleData;
}

module.exports = {
    generate_sample_data: generate_sample_data
};
