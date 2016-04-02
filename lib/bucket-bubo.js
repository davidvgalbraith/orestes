'use strict';

var Bubo = require('object-hash-set');

class BucketBubo {
    constructor(options) {
        this.buboOptions = options;
        this.bubos = {};
    }

    add(bucket, object, result) {
        this.bubos[bucket] = this.bubos[bucket] || new Bubo(this.buboOptions);
        return this.bubos[bucket].add(object, result);
    }

    delete(bucket, object) {
        this.bubos[bucket] = this.bubos[bucket] || new Bubo(this.buboOptions);
        return this.bubos[bucket].delete(object);
    }

    delete_bucket(bucket) {
        delete this.bubos[bucket];
    }

    get_buckets() {
        return Object.keys(this.bubos);
    }
}

module.exports = BucketBubo;
