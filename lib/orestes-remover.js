var Promise = require('bluebird');
var util = require('util');
var utils = require('./orestes-utils');
var cassUtils = require('./cassandra').utils;
var electra_utils = require('./elasticsearch/utils');

var msInDay = 1000 * 60 * 60 * 24;
var cassandra_client, es_url, space_info;

function init(config, cassandraClient) {
    cassandra_client = cassandraClient;
    es_url = 'http://' + config.elasticsearch.host + ':' + config.elasticsearch.port + '/';
    space_info = config.spaces;
}

function remove(query) {
    var space = query.space;
    var today = Math.floor(Date.now() / msInDay);
    var doomedDay = today - query.keep_days;  // latest day to delete

    utils.clearCaches(space, doomedDay);
    var granularity = space_info[space].table_granularity_days;

    return Promise.all([
        utils.getAllTablesForSpace(space),
        electra_utils.listIndices(es_url)
    ])
    .spread(function(tables, indices) {
        var tablesToDelete = tables.filter(function shouldDropTable(table) {
            var end = Math.min(utils.dayFromOrestesTable(table) + granularity-1, today);
            return end <= doomedDay;
        });

        var indexesToDelete = Object.keys(indices).filter(function shouldDropIndex(index) {
            var end = Math.min(utils.dayFromIndex(index) + granularity-1, today);
            return index.indexOf('metadata') === 0 &&
                utils.spaceFromIndex(index) === query.space &&
                end <= doomedDay;
        });

        return Promise.all([
            Promise.each(tablesToDelete, function deleteTable(table) {
                var table_name = util.format('%s.%s', utils.orestesKeyspaceName(space), table);
                var cql = util.format('DROP TABLE %s;', table_name);
                return cassUtils.awaitTable(table_name)
                    .then(function() {
                        return cassandra_client.execute(cql);
                    });
            }),
            electra_utils.synchronousDeleteIndices(es_url, indexesToDelete)
        ]);
    });
}

module.exports = {
    remove: remove,
    init: init
};
