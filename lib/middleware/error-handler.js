var Logger = require('../../logger');

var logger = Logger.get('error-handler');

var DEFAULT_STATUS = 500;

// internal error status
var INTERNAL_ERROR = DEFAULT_STATUS;

module.exports = function () {
    return function (err, req, res, next) {
        var status = error_status(err);
        if (status === INTERNAL_ERROR) {
            log_error(err, 'error');
        } else {
            log_error(err, 'debug');
        }

        res.setHeader("Content-Type", "application/json");
        res.statusCode = status;
        res.end(JSON.stringify(error_to_json(err)));
    };
};

function error_status(err) {
    return err.status ? err.status : DEFAULT_STATUS;
}

function error_to_json(err) {
    return err.to_json ? err.to_json() : {
        code: 'INTERNAL',
        message: err.message,
        info: {}
    };
}

function log_error(err, severity) {
    var action = err.action;
    var context = err.context;

    var message = action ?
        action + ' error' : 'internal error';

    var args = [message];
    if (context) {
        args.push(context);
    }
    args.push(err);

    logger[severity].apply(logger, args);
}
