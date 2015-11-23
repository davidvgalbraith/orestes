
var Base = require('extendable-base');

var ElasticsearchException = Base.inherits(Error, {
    initialize: function(exception) {
        this.exception = exception;
    }
} );

var MissingField = Base.inherits(Error, {
    initialize: function(name) {
        this.name = name;
    }
});

var ContextMissing = Base.inherits(Error, {});
var AllFailed = Base.inherits(Error, {});
var ScriptMissing = Base.inherits(Error, {});

var _ES_EXCEPTION_PATTERN = /ElasticsearchException\[([A-Za-z.]+):/;
var _MISSING_FIELD_PATTERN = /GroovyScriptExecutionException\[ElasticsearchIllegalArgumentException\[No field found for \[([^\]]+)\] in mapping/;
var _ALL_FAILED_PATTERN = /SearchPhaseExecutionException\[Failed to execute phase \[query(_fetch)?\], all shards failed\]/;
var _MISSING_CONTEXT_PATTERN = /SearchContextMissingException\[No search context found/;
var _SCRIPT_MISSING_PATTERN = /Unable to find on disk script/;

function categorize_error(error) {
    var str = error && error.root_cause && error.root_cause[0] && error.root_cause[0].reason;
    if (!str) {
        return null;
    }
    var match = str.match(_ES_EXCEPTION_PATTERN);
    if (match) {
        return new ElasticsearchException(match[1]);
    }

    match = str.match(_MISSING_FIELD_PATTERN);
    if (match) {
        return new MissingField(match[1]);
    }

    if (str.match(_ALL_FAILED_PATTERN)) {
        return new AllFailed();
    }

    if (str.match(_MISSING_CONTEXT_PATTERN)) {
        return new ContextMissing();
    }

    if (str.match(_SCRIPT_MISSING_PATTERN)) {
        return new ScriptMissing();
    }

    return null;
}

module.exports = {
    categorize_error: categorize_error,
    ElasticsearchException: ElasticsearchException,
    MissingField: MissingField,
    ContextMissing: ContextMissing,
    ScriptMissing: ScriptMissing,
    AllFailed: AllFailed
};
