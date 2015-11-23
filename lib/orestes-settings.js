var CassandraClient = require('./cassandra').client;

var value_type = {
    string: 'double',
    code: CassandraClient.types.CASS_VALUE_TYPE_DOUBLE
};

var fields = {
    attrs: 'varchar',
    offset: 'int',
    value: value_type.string
};

var key = 'attrs, offset';

module.exports = {
    // the type information for the value field in our Cassandra tables
    value_type: value_type,
    unknown_type: CassandraClient.types.CASS_VALUE_TYPE_UNKNOWN,
    int_type: CassandraClient.types.CASS_VALUE_TYPE_INT,
    string_type: CassandraClient.types.CASS_VALUE_TYPE_VARCHAR,
    timestamp_type: CassandraClient.types.CASS_VALUE_TYPE_TIMESTAMP,
    counter_type: CassandraClient.types.CASS_VALUE_TYPE_COUNTER,
    primary_key: key,
    table_fields: fields,
    KEYSPACE_PREFIX: 'orestes_',
    TABLE_NAME: 'metrics'
};
