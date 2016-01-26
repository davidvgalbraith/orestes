var request = require('request');
var Promise = require('bluebird');
var OrestesSettings = require('./orestes-settings');
var utils = require('./orestes-utils');
var valueCode = OrestesSettings.value_type.code;
var string = OrestesSettings.string_type;
var int = OrestesSettings.int_type;
var logger = require('../logger').get('orestes-inserter');
var cass_errors = require('./cassandra').errors;

var cassandra_client, bubo, es_url, space_info;
var MS_IN_DAY = 1000 * 60 * 60 * 24;
var BATCH_SIZE = 100;
var ES_SUCCESSFUL_CREATE_CODE = 201;
var ES_DOCUMENT_EXISTS_CODE = 409;

function item_is_failure(es_resp_item) {
    var create = es_resp_item && es_resp_item.create;
    return !create || create.status !== ES_SUCCESSFUL_CREATE_CODE && create.status !== ES_DOCUMENT_EXISTS_CODE;
}

var param_types = {
    param_types: [string, int, valueCode]
};
var buboResult = {};

function OrestesInserter(options) {
    // An entry in this.metadata is a point for which we're going to insert metadata to ES
    // the entry at the same index in this.attrStrings is the Cassandra rowkey for that point
    // and the corresponding entry in this.buckets is the week for that point
    // this is a performance optimization to minimize object allocations
    this.metadata = [];
    this.attrStrings = [];
    this.buckets = [];

    this.total = 0;
    this.batches = [];
    this.active_batch = cassandra_client.new_batch('unlogged');
    this.space = options.space;
    this.prepareds = options.prepareds;
    this.bubo = bubo;
    this.errors = [];
}

function init(config, bubo_cache, cassandraClient) {
    cassandra_client = cassandraClient;
    bubo = bubo_cache;
    es_url = 'http://' + config.elasticsearch.host + ':' + config.elasticsearch.port + '/_bulk';
    space_info = config.spaces;
}

OrestesInserter.prototype.push = function(pt) {
    try {
        this._push(pt);
    } catch(err) {
        this.errors.push({
            point: pt,
            error: err.message
        });
    }
};

OrestesInserter.prototype._push = function(pt) {
    this.schema = this.schema || pt.source_type;
    if (this.schema !== pt.source_type) { // can also be removed if/when import API changes
        throw new Error('Can only import to one schema per insert');
    }

    var today = Math.floor(pt.time / MS_IN_DAY);
    var bucket = utils.roundToGranularity(today, this.space);
    var prepared = this.prepareds[bucket];
    var offset = pt.time % (MS_IN_DAY * space_info[this.space].table_granularity_days);

    var attrString = this.validate_and_handle_metadatum(pt, bucket);

    this.active_batch.add_prepared(prepared, [attrString, offset, pt.value], param_types);

    this.total++;

    if (this.total % BATCH_SIZE === 0) {
        this.batches.push(execute_batch(this.active_batch));
        this.active_batch = cassandra_client.new_batch('unlogged');
    }
};

OrestesInserter.prototype.end = function() {
    var self = this;
    if (this.total % BATCH_SIZE !== 0) {
        this.batches.push(execute_batch(this.active_batch));
    }

    return Promise.all(this.batches.concat([this.insert_metadata()]))
    .catch(function(err) {
        logger.error('error during import:', err);

        throw cass_errors.categorize_error(err, 'batch_insert');
    });
};

OrestesInserter.prototype.validate_and_handle_metadatum = function(pt, bucket) {
    this.bubo.add(this.space + '@' + bucket, pt, buboResult);
    var attrString = buboResult.attr_str;

    if (!buboResult.found) {
        try {
            this.metadata.push(utils.getValidatedStringifiedPoint(pt, attrString));
            this.attrStrings.push(attrString);
            this.buckets.push(bucket);
        } catch(err) {
            this.bubo.delete(this.space + '@' + bucket, pt);
            throw err;
        }
    } else {
        validateValue(pt);
    }

    return attrString;
};

OrestesInserter.prototype.build_es_request = function() {
    var requestBody = '';
    for (var k = 0; k < this.metadata.length; k += 1) {
        var createCmd = JSON.stringify({
            create: {
                _index: utils.metadataIndexName(this.space, this.buckets[k]),
                _type: 'metric',
                _id: this.attrStrings[k]
            }
        });

        requestBody += createCmd + '\n' + this.metadata[k] + '\n';
    }

    return requestBody;
};

OrestesInserter.prototype.remove_metadata_from_bubo = function() {
    for (var k = 0; k < this.metadata.length; k += 1) {
        var metadatum = JSON.parse(this.metadata[k]);
        var bucket = this.buckets[k];
        this.bubo.delete(this.space + '@' + bucket, metadatum);
    }
};

OrestesInserter.prototype.insert_metadata = function() {
    var self = this;
    var requestBody = this.build_es_request();

    if (requestBody.length === 0) { return Promise.resolve(); }

    return request.postAsync({
        url: es_url,
        body: requestBody
    })
    .spread(function(res, body) {
        var items = JSON.parse(body).items;
        for (var i = 0; i < items.length; i++) {
            if (item_is_failure(items[i])) {
                var failed_metadatum = JSON.parse(self.metadata[i]);
                self.bubo.delete(self.space + '@' + self.buckets[i], failed_metadatum);
                self.errors.push({pt: failed_metadatum, error: create ? create.error : 'unknown'});
            }
        }
    })
    .catch(function(err) {
        // if metadata insertion to ES fails, we have to clear the metadata
        // cache for these points or else they'll never get written
        self.remove_metadata_from_bubo();
        throw err;
    });
};

function _execute_batch(batch, cb) {
    batch.execute({}, cb);
}

var execute_batch = Promise.promisify(_execute_batch);

function validateValue(metric) {
    var v = metric.value;
    if (typeof v !== 'number' || v !== v) {
        throw new Error('invalid value ' + v);
    }
}

function insert(points, space) {
    var inserter;
    return utils.getImportPrepareds(space, points)
        .then(function(prepareds) {
            inserter = new OrestesInserter({
                prepareds: prepareds,
                space: space
            });

            points.forEach(function(pt) {
                inserter.push(pt);
            });

            return inserter.end();
        })
        .then(function() {
            return {errors: inserter.errors};
        });
}

module.exports = {
    insert: insert,
    init: init
};
