// In the browser the default level is set to warning so that we reduce
// the console spew.
var default_config = {
    levels: {
        "*": "warn"
    }
};

var config;

var storage_key = 'jut-logger-config';

function load() {
    /* global localStorage, console */
    try {
        var stored = localStorage.getItem(storage_key);
        if (stored) {
            config = JSON.parse(stored);
        } else {
            config = default_config;
        }
    } catch(err) {
        console.error('error loading logger config: ', err);
    }
    return config;
}

function save() {
    localStorage.setItem(storage_key, JSON.stringify(config));
}

function reset() {
    config = default_config;
    localStorage.removeItem(storage_key);
}

function set_level(name, level) {
    config.levels[name] = level;
    save();
}

module.exports = {
    load: load,
    save: save,
    reset: reset,
    set_level: set_level
};
