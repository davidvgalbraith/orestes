var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
var retry = require('bluebird-retry');

var logger = require('logger').get('elasticsearch-delete');

// takes an ES response blob and returns the _source fields augmented with the
// source_type
function pointsFromESDocs(hits) {
    return hits.map(function(hit) {
        var point = hit._source;
        point.source_type = es_type_to_source_type(hit._type);

        // read's heapsort loop demands points with times in milliseconds
        // because that's fast for metrics
        point.time = new Date(point.time).getTime();
        return point;
    });
}


var SCHEMA_RE = /^schema-(.*)$/;
function es_type_to_source_type(type) {
    var m = type.match(SCHEMA_RE);
    return m ? m[1] : type;
}

function synchronousDeleteIndices(es_url, indices) {
    return Promise.each(indices, function(index) {
        logger.info('deleting', deleteOpts.url);
        var url = es_url + index;

        return request.deleteAsync(url)
        .spread(function(res, body) {
            // sometimes it takes a while for elasticsearch to delete an index
            // we want to make sure the indices are gone before we continue
            return retry(function() {
                return request.headAsync(url)
                .spread(function(res, body) {
                    if (res.statusCode !== 404) {
                        throw new Error('Index not yet deleted, have to try again');
                    }
                });
            }, {max_tries: 10, interval: 3000});
        });
    });
}

module.exports = {
    synchronousDeleteIndices: synchronousDeleteIndices,
    pointsFromESDocs: pointsFromESDocs,
    sourceTypeFromESDoc: es_type_to_source_type
};
