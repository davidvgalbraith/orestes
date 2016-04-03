function categorize_error(err) {
    if (err.message.match(/Cannot achieve consistency level/) ||
       err.message.match(/All connections on all I\/O threads are busy/)) {
        err.status = 503;
    }

    return err;
}

module.exports = {
    categorize_error: categorize_error
};
