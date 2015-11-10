var _ = require('underscore');
var Promise = require('bluebird');
var OrestesSettings = require('./orestes-settings');
var cassUtils = require('./cassandra').utils;
var metricsTableName = OrestesSettings.TABLE_NAME;
var keyspacePrefix = OrestesSettings.KEYSPACE_PREFIX;
var msInDay = 1000 * 60 * 60 * 24;
var bubo;
var logger = require('logger').get('orestes');

var prepareds = {};
var orestesTableConfig;

var METADATA_GRANULARITY = 1; // dave get rid of this you fool

var preparedBases = {
    select: 'SELECT offset, value FROM %s.%s WHERE attrs = ? AND offset >= ? AND offset < ?;',
    count: 'SELECT COUNT(*) FROM %s.%s WHERE attrs = ? AND offset >= ? AND offset < ?;',
    import: 'INSERT INTO %s.%s (attrs, offset, value) VALUES (?, ?, ?);'
};

var table_options = {
    compact_storage: true,
    bloom_filter_fp_chance: 0.010000,
    comment: '',
    dclocal_read_repair_chance: 0.000000,
    gc_grace_seconds: 864000,
    read_repair_chance: 1.000000,
    default_time_to_live: 0,
    speculative_retry: 'NONE',
    memtable_flush_period_in_ms: 0,
    compaction: {class: 'SizeTieredCompactionStrategy', cold_reads_to_omit: 0.0},
    compression: {sstable_compression: 'LZ4Compressor'}
};

function init(config, cassandraClient, bubo_cache) {
    bubo = bubo_cache;

    cassUtils.init(cassandraClient, prepareds);

    var tableOptions = cassUtils.buildOptsString(table_options);
    logger.info('table options string', tableOptions);

    orestesTableConfig = {
        table_fields: OrestesSettings.table_fields,
        primary_key: OrestesSettings.primary_key,
        table_options: tableOptions
    };
}

// we store metrics in Cassandra with a attrs key consisting of the tag series
// column names equal to timestamps and column values as doubles
// unpack that structure into Javascript objects
// for performance, store the parsed-out tags for each attrs in the "known" object
function uncompactRows(rows, week, tags) {
    var weekMs = week * msInDay;
    return rows.map(function makeJuttlePoint(cassPoint) {
        var uncompactedPoint = {
            time: weekMs + cassPoint.offset,
            value: cassPoint.value,
            source_type: 'metric'
        };

        // the speediest known way to _.extend
        for (var key in tags) {
            uncompactedPoint[key] = tags[key];
        }

        return uncompactedPoint;
    });
}

// rounds n to the nearest multiple of our metadata granularity
function roundToGranularity(n) {
    return Math.floor(n / METADATA_GRANULARITY) * METADATA_GRANULARITY;
}

function spaceFromIndex(_index) {
    return _index.substring(_index.indexOf('-') + 1, _index.indexOf('@'));
}

function dayFromIndex(_index) {
    return parseInt(_index.substring(_index.indexOf('@') + 1));
}

function orestesKeyspaceName(space) {
    return '"' + keyspacePrefix + space + '"';
}

function orestesTableName(day) {
    return metricsTableName + day;
}

function getOrestesPrepared(space, day, type) {
    var preparedOptions = _.extend({
        keyspace: orestesKeyspaceName(space),
        columnfamily: orestesTableName(day),
        cql: preparedBases[type]
    }, orestesTableConfig);

    return cassUtils.getPrepared(preparedOptions);
}

function normalize_timestamp(ts) {
    if (typeof ts === 'string') {
        // XXX if we only want to support ISO 8601, we should
        // have a parser for that, Date.parse() will accept other
        // formats as well...
        ts = Date.parse(ts);
    }

    if (typeof ts !== 'number' || ts !== ts) { // speedy hack for isNaN(ts)
        throw new errors.MalformedError('invalid timestamp');
    }

    return new Date(ts);
}

function getImportPrepareds(space, points) {
    var days = {};
    points.forEach(function(pt) {
        try {
            pt.time = normalize_timestamp(pt.time).getTime();
        } catch (err) {
            // catch it later
            return;
        }

        var day = roundToGranularity(Math.floor(pt.time / msInDay));
        if (!days[day]) {
            days[day] = 1;
        }
    });

    days = _.keys(days);
    return Promise.map(days, function(day) {
        return getOrestesPrepared(space, day, 'import');
    })
    .then(function(prepped) {
        var ret = {};
        days.forEach(function(day, i) {
            ret[day] = prepped[i];
        });
        return ret;
    });
}

function dayFromOrestesTable(table) {
    var numStart = table.indexOf(metricsTableName) + metricsTableName.length;
    return Number(table.substring(numStart));
}

function metadataIndexName(space, day) {
    return 'metadata-' + space + '@' + day;
}

function clearOrestesPrepareds(space, deleteDay) {
    var keyspace = orestesKeyspaceName(space);
    _.each(prepareds[keyspace], function(prepped, cfName) {
        if (dayFromOrestesTable(cfName) <= deleteDay) {
            prepareds[keyspace][cfName] = null;
        }
    });
}

// to be called on delete, blows away our cached information
// for deleted tables
function clearCaches(space, deleteDay) {
    clearOrestesPrepareds(space, deleteDay);
    bubo.remove_bucket(space + '@' + deleteDay);
}

function weekFromTable(table) {
    return Number(table.substring(metricsTableName.length));
}

var defaultUnwantedTags = {
    time: true,
    space: true,
    value: true,
    source_type: true
};

function getAttributeString(pt, unwantedTags) {
    unwantedTags = unwantedTags || defaultUnwantedTags;
    var tagNames = [];
    _.each(pt, function(tagValue, tagName) {
        if (!unwantedTags[tagName]) {
            tagNames.push(tagName);
        }
    });

    tagNames.sort();

    var attrString = '';
    var comma = '';
    for (var tag = tagNames.length - 1; tag >= 0; tag--) {
        attrString = tagNames[tag] + '=' + pt[tagNames[tag]] + comma + attrString;
        comma = ',';
    }

    return attrString;
}

// this is inaesthetic but Object.keys() is not cheap so we
// combine the logically distinct operations that iterate
// over the keys of the point for performance
function getValidatedStringifiedPoint(pt, attrs) {
    if (!pt.hasOwnProperty('time') || !pt.hasOwnProperty('value')) {
        cass_utils.validateHasAll(pt, ['time', 'value']);
    }
    if (attrs.length === 0) {
        throw new Error('metrics must have at least one tag');
    }
    // the second argument to JSON.stringify is a whitelist of keys to stringify
    return JSON.stringify(pt, Object.keys(pt).filter(function(key) {
        var value = pt[key];
        if (key === 'value') {
            if (typeof value !== 'number' || value !== value) { // speedy hack for isNaN(value)
                throw new Error('invalid value ' + value);
            }
        } else {
            // disallow any points with nested structure (typeof null === 'object' so we check truthiness too)
            if (value && typeof value === 'object') {
                throw new Error('invalid tag - value is an object or array ' + key + ' : ' + value);
            }
        }

        return !defaultUnwantedTags[key];
    }));
}

function getAllTablesForSpace(space) {
    var keyspace = orestesKeyspaceName(space);
    return cassUtils.getAllTablesForKeyspace(keyspace);
}

module.exports = {
    init: init,
    roundToGranularity: roundToGranularity,
    spaceFromIndex: spaceFromIndex,
    dayFromIndex: dayFromIndex,
    getPrepared: getOrestesPrepared,
    getImportPrepareds: getImportPrepareds,
    getAttributeString: getAttributeString,
    dayFromOrestesTable: dayFromOrestesTable,
    metadataIndexName: metadataIndexName,
    weekFromTable: weekFromTable,
    orestesKeyspaceName: orestesKeyspaceName,
    clearCaches: clearCaches,
    getAllTablesForSpace: getAllTablesForSpace,
    getValidatedStringifiedPoint: getValidatedStringifiedPoint,
    buboOptions: {
        ignoredAttributes: defaultUnwantedTags
    },
    normalize_timestamp: normalize_timestamp,
    uncompactRows: uncompactRows
};
