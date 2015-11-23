var Base = require('extendable-base');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
var url = require('url');
var retry = require('bluebird-retry');

var Logger = require('../../logger');

var query = require('./query');
var aggregation = require('./aggregation');

var Electra = Base.extend({
    initialize: function(config) {
        this.logger = Logger.get('electra');

        this.es_url = url.format({
            protocol: 'http',
            hostname: config.elasticsearch.host,
            port: config.elasticsearch.port
        });
    },

    _connect_to_es: function() {
        // check that es is running by hitting the /version endpoint
        var self = this;
        return request.getAsync({
            url: this.es_url,
            json: true
        }).spread(function(response, body) {
            if (response.statusCode !== 200) {
                throw new Error('cannot read version, got status ' +
                response.statusCode);
            } else {
                self.logger.info('elasticsearch validation response:', body);
            }
        })
        .catch(function(err) {
            self.logger.info('waiting for elasticsearch to start...', {message: err.message});
            throw err;
        });
    },

    startup: function() {
        var self = this;

        this.logger.info('startup, elasticsearch at ', self.es_url);

        return retry(function() {
            return self._connect_to_es();
        }, { max_tries: Infinity })
        .catch(function(err) {
            self.logger.error('error validating elasticsearch:', err.message);
            throw err;
        });
    },
},
{
    init: function(config) {
        query.init(config);
    }
});

module.exports = Electra;
