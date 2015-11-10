var request = require('request');
var Promise = require('bluebird');
var shared = require('../import/import-shared');
var Metrics = require('metrics');
var OrestesConfig = require('./orestes-config');
var utils = require('./orestes-utils');
var valueCode = OrestesConfig.value_type.code;
var string = OrestesConfig.string_type;
var int = OrestesConfig.int_type;
var logger = require('logger').get('orestes-inserter');
var errs = require('../import/import-errors');
var cass_errors = require('../cassandra').errors;
var Schemas = require('../electra/schemas');


var SAMPLE_RATE, cassandra_client, bubo, es_url, metrics, METADATA_GRANULARITY;
var MS_IN_DAY = 1000 * 60 * 60 * 24;
var BATCH_SIZE = 100;
var preparedHints = {
    hints: [string, int, valueCode]
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
    this.want_result_details = options.want_result_details;
    this.space = options.space;
    this.timer = metrics.create_timer('orestes.insert', SAMPLE_RATE);
    this.prepareds = options.prepareds;
    this.bubo = bubo;
}

function init(config, bubo_cache, cassandraClient, metrics_) {
    SAMPLE_RATE = config.get('sample_rate');
    cassandra_client = cassandraClient;
    bubo = bubo_cache;
    es_url = 'http://' + config.get('elasticsearch:host') + ':' + config.get('elasticsearch:port') + '/_bulk';
    metrics = metrics_ || new Metrics();
    METADATA_GRANULARITY = config.get('orestes').metadata_granularity_days;
}

OrestesInserter.prototype.push = function(pt) {
    pt.time = shared.normalize_timestamp(pt.time).getTime();
    this.schema = this.schema || pt.source_type;
    if (this.schema !== pt.source_type) { // can also be removed if/when import API changes
        throw new Error('Can only import to one schema per insert');
    }

    var today = Math.floor(pt.time / MS_IN_DAY);
    var bucket = utils.roundToGranularity(today);
    var prepared = this.prepareds[bucket];
    var offset = pt.time % (MS_IN_DAY * METADATA_GRANULARITY);

    var attrString = this.validate_and_handle_metadatum(pt, bucket);

    this.active_batch.add_prepared(prepared, [attrString, offset, pt.value], preparedHints);
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

    metrics.count('cassandra.request.op__batch_insert', this.batches.length);
    metrics.count('cassandra.record.op__batch_insert', this.total);

    return Promise.all(this.batches.concat([this.insert_metadata()]))
    .then(function() {
        var result = {
            success: self.total,
            fail: 0
        };

        if (self.want_result_details) {
            var details = [];
            for (var k = 0; k < self.total; k++) {
                details[k] = shared.success;
            }
            result.details = details;
        }

        return result;
    })
    .catch(function(err) {
        logger.error('error during import:', err);

        throw cass_errors.categorize_error(err, 'batch_insert');
    })
    .finally(function() {
        self.timer.stop();
    });
};

OrestesInserter.prototype.validate_and_handle_metadatum = function(pt, bucket) {
    this.bubo.lookup_point(this.space + '@' + bucket, pt, buboResult);
    var attrString = buboResult.attr_str;

    if (!buboResult.found) {
        try {
            this.metadata.push(utils.getValidatedStringifiedPoint(pt, attrString));
            this.attrStrings.push(attrString);
            this.buckets.push(bucket);
        } catch(err) {
            this.bubo.remove_point(this.space + '@' + bucket, pt);
            throw err;
        }
    } else {
        validateValue(pt);
    }

    return attrString;
};

OrestesInserter.prototype.insert_metadata = function() {
    var self = this;
    var requestBody = '';
    for (var k = 0; k < this.metadata.length; k += 1) {
        var createCmd = JSON.stringify({
            create: {
                _index: utils.metadataIndexName(self.space, this.buckets[k]),
                _type: Schemas.mapping_name(self.schema),
                _id: this.attrStrings[k]
            }
        });

        requestBody += createCmd + '\n' + this.metadata[k] + '\n';
    }

    metrics.increment('elasticsearch.request.op__insert_metadata', SAMPLE_RATE);
    metrics.count('elasticsearch.record.op__insert_metadata', this.metadata.length, SAMPLE_RATE);

    if (requestBody.length > 0) {
        return request.postAsync({
            url: es_url,
            body: requestBody
        })
        .catch(function(err) {
            // if metadata insertion to ES fails, we have to clear the metadata
            // cache for those points or else they'll never get written
            for (var k = 0; k < self.metadata.length; k += 1) {
                var metadatum = JSON.parse(self.metadata[k]);
                var bucket = self.buckets[k];
                self.bubo.remove_point(self.space + '@' + bucket, metadatum);
            }
            throw err;
        });
    } else {
        return Promise.resolve();
    }
};

function _execute_batch(batch, cb) {
    batch.execute({}, cb);
}

var execute_batch = Promise.promisify(_execute_batch);

function validateValue(metric) {
    var v = metric.value;
    if (typeof v !== 'number' || v !== v) {
        throw new errs.MalformedError('invalid value ' + v);
    }
}

module.exports = {
    inserter: OrestesInserter,
    init: init
};
