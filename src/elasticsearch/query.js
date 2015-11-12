
var _ = require('underscore');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));

var JuttleMoment = require('juttle-moment').JuttleMoment;
var errors = require('./es-errors');

var Logger = require('logger');
var logger = Logger.get('elasticsearch');

var es_config;

var SEARCH_SUFFIX = '/_search';
var DEFAULT_PARAMETERS = {ignore_unavailable: true};

// Ouch, ES uses a fixed 4KB buffer for each line of HTTP input.
// The very first line contains a method, a path, and the string
// HTTP/1.1 (8 character).  The longest method name we use
// is OPTIONS (7 characters).  Then there are spaces separating
// those components.  So up to 15 bytes are use for stuff that
// isn't part of the path leaving:
var MAX_ES_URL_LENGTH = 4095 - 15;

function build_orestes_url(space, days, params, suffix) {
    return _build_url('metadata-', space, days, suffix, _.extend(params, DEFAULT_PARAMETERS));
}

function build_scroll_url() {
    var parameters = _.extend({
        scroll: '10m',
    }, DEFAULT_PARAMETERS);
    return _build_url(null, null, null, '_search/scroll', parameters);
}

function _build_url(prefix, space, dates, suffix, parameters) {
    suffix = suffix || SEARCH_SUFFIX;
    parameters = parameters || DEFAULT_PARAMETERS;

    var indices = [];
    _.each(dates, function(dateString) {
        indices.push(prefix + space + '@'+ dateString);
    });

    var str = indices.join(',');

    var parameter_string = _.map(parameters, function(value, key) {
        return key + '=' + value;
    }).join('&');

    if (parameter_string.length > 0) {
        parameter_string = '?' + parameter_string;
    }

    var path = str + suffix + parameter_string;

    // if the combination of indices and mappings and parameters
    // is too long, then use wildcards for indices.
    if (path.length > MAX_ES_URL_LENGTH) {
        str = prefix + space + '@*';
        path = str + suffix + parameter_string;
    }

    return 'http://' + es_config.host + ':' + es_config.port + '/' + path;
}

function getTimeIndices(from, to) {
    var maxDays = JuttleMoment.duration(14, 'day');

    to = to || new JuttleMoment();

    if (from === null || JuttleMoment.add(from, maxDays).lt(to)) {
        return ['*'];
    }

    var day = JuttleMoment.duration(1, 'day');

    var strings = [];

    var current = from.quantize(JuttleMoment.duration(1, 'day'));
    var max = to.quantize(JuttleMoment.duration(1, 'day'));

    while (current.lte(max)) {
        // Push only the first part of the ISO string (e.g. 2014-01-01).
        strings.push(current.valueOf().substr(0, 10).replace(/-/g, '.'));

        current = JuttleMoment.add(current, day);
    }

    return strings;
}

function execute(url, body, method, options) {
    options = options || {};
    return request.getAsync({
        url: url,
        method: method || 'POST',
        json: body
    })
    .cancellable()
    .spread(function(response, body) {
        if (response.statusCode !== 200 && response.statusCode !== 201) {
            var err = errors.categorize_error(body.error);
            if (err instanceof errors.MissingField ||  err instanceof errors.ScriptMissing) {
                throw err;
            }

            // ugh, if we query a brand new index, we occassionally
            // get this error.  just treat it as empty results.
            if (err && err instanceof errors.AllFailed) {
                return {
                    hits: {
                        total: 0,
                        hits: []
                    }
                };
            }

            if (response.statusCode >= 400) {
                logger.warn('ES error response', response.statusCode, body);
            }

            err = new Error('Received status code ' + response.statusCode + ' from ElasticSearch');
            err.status = response.statusCode;
            throw err;
        }

        // oh dear this is a hack
        if (url.indexOf('_search') !== -1 && body._shards && body._shards.failures) {
            var IGNORE_KEY = '**ignore**';
            var counts = _.countBy(body._shards.failures, function(failure) {
                var err = errors.categorize_error(failure.reason);

                if (err && err instanceof errors.MissingField) {
                    // XXX see PROD-7325 for discussion about why we skip these
                    return IGNORE_KEY;
                }
                else if (err && err instanceof errors.ElasticsearchException) {
                    return err.exception;
                } else if (err && err instanceof errors.ContextMissing) {
                    logger.warn('attempted to read from deleted index');
                    return IGNORE_KEY;
                } else {
                    return failure.reason;
                }

            });

            var total = 0;
            _.each(counts, function(n, what) {
                if (what !== IGNORE_KEY) {
                    total += n;
                    logger.error('Got exception ' + what + ' on ' + n + ' shards');
                }
            });
            if (total > 0) {
                throw new Error('Your query flummoxed Elasticsearch, see elasticsearch.log for stack trace details');
            }
        }

        return body;
    });
}

function init(config) {
    es_config = config.elasticsearch;
}

module.exports = {
    init: init,
    MAX_ES_URL_LENGTH: MAX_ES_URL_LENGTH,
    build_orestes_url: build_orestes_url,
    build_scroll_url: build_scroll_url,
    execute: execute
};
