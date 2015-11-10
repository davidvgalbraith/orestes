var request = require('request-async');
var Promise = require('bluebird');
var _ = require('underscore');
var util = require('util');
var utils = require('./orestes-utils');
var cassUtils = require('../cassandra').utils;
var electra_utils = require('../electra/utils');

var msInDay = 1000 * 60 * 60 * 24;
var cassandra_client, es_url, METADATA_GRANULARITY, communicator;

function init(config, cassandraClient, comm) {
    cassandra_client = cassandraClient;
    communicator = comm;
    METADATA_GRANULARITY = config.get('orestes').metadata_granularity_days;
    es_url = 'http://' + config.get('elasticsearch').host + ':' + config.get('elasticsearch').port + '/';
}

function remove(query) {
    if (query.dry_run) {
        return Promise.resolve();
    }

    var space = query.space;
    var listIndexesUrl = es_url + '_aliases';
    var today = Math.floor(Date.now() / msInDay);
    var doomedDay = today - query.keep_days;  // latest day to delete

    if (communicator) {
        communicator.broadcast('deletion', {
            day: doomedDay,
            space: space
        });
    }

    utils.clearCaches(space, doomedDay);

    // get a list of all indexes
    return Promise.all([
            utils.getAllTablesForSpace(space),
            request.async(listIndexesUrl).spread(function(res, body) { return body; })
        ])
        .spread(function(tables, indices) {
            var tablesToDelete = tables.filter(function(table) {
                var end = Math.min(utils.dayFromOrestesTable(table) + METADATA_GRANULARITY-1, today);
                return end <= doomedDay;
            });

            var indexesToDelete = _.keys(JSON.parse(indices)).filter(function(index) {
                var end = Math.min(utils.dayFromIndex(index) + METADATA_GRANULARITY-1, today);
                return index.indexOf('metadata') === 0 &&
                        utils.spaceFromIndex(index) === query.space &&
                        end <= doomedDay;
            });

            return Promise.all([
                Promise.each(tablesToDelete, function(table) {
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
