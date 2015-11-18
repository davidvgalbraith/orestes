# Logger

The logger is the core module used for debug and output logging in the
Jut system.

It is used in both the browser and the server code, and supports a
simple API for creating a log target with a given string identifier,
and runtime configuration to control the output level for a given log
prefix.

For now it only outputs to console.log but eventually we expect to
support other configurable logging targets, including notably sending
the logs to Jut itself for analytics.

## Basic Usage

Obtain a new logger for a portion of the code called 'app-main':

```javascript
var Logger = require('../../logger');
var logger = Logger.get('app.main')
```

Then use the logger instance to output logging at various levels. Each
function supports the same calling conventions as console.log to
concatenate arguments.

```javascript
// app.main: this is a test message at Fri Jan 03 2014 10:26:35 GMT-0800 (PST)
logger.info('this is a test message at', new Date())

// app.main: something bad might happen
logger.warn('something bad might happen')

// app.main: something bad happened
logger.error('something bad happened')


logger.debug('debug suppressed by default')
```

The above methods all have versions with `Async` appended that
return a promise that will be resolved only after the log message
has been completely written.  This is particularly useful for
logging something before exiting:

```javascript
logger.errorAsync('fatal error!')
.then(function() { process.exit(-1); });
```

## Output Level Configuration

By default, logging is enabled for all targets at levels `info`,
`warn`, and `error`, and is disabled for `debug`.

The log level target can be overridden to either increase or decrease
this target level.

### Level Patterns

The levels map controls the output level for each logger, based on a map
of logger name prefix to the appropriate level for the logger.

A special prefix value of `*` overrides the default level.

For example, the following config would enable `app.main` logs at
debug level, `app` logs at info or higher level, `search` logs at
error level only, and all other logs at warning or higher:

```json
{
    "levels": {
        "*" : "warn",
        "app.main" : "debug",
        "app" : "info",
        "search" : "error"
    }
}
```

### Server configuration: loading logger-config.json

In a node.js process, the first time a log target is instantiated
using `Logger.get`, it loads its configuration from disk.

If the LOGGER_CONFIG environment variable exists, then it is expected to
contain the path of the config file.

If not, the module searches for the `logger-config.json` file, starting
in the current working directory and proceeding up the filesystem
hierarchy until it either finds a matching file or reaches the root.
In practice, this means that a developer can put the config file
either at the top level of the repository, in the developer's home
directory, or at the root of the filesystem.

### Browser configuration: using localStorage

In the browser, the level map is stored in localStorage, and a simple API is
exposed to developers via a `JutLogger` global. By default, the logging level
is set to "warn" to reduce the verbosity in the console.

However, to override the level for a given logger, a developer can go to the
console:

```
JutLogger.set_level('app.main', 'debug');
```

This will set the runtime level for all loggers that match 'app.main', and will
store the pattern in local storage so that the next time the application is
run, the setting will be persistent.

### Server-side transport configuration

`logger-config.json` also contains `transports`. A `transport` is essentially
a storage device for logs. Examples include `console`, `file`, `syslog`, and `dailyRotateFile`:

```json
{
   "levels":{
      "*":"info"
   },
   "transports":{
       "console":{
         "json": false,
         "label": "",
         "timestamp": true,
         "prettyPrint": false,
       },
       "file":{
         "colorize":false,
         "timestamp":true,
         "filename":"/var/log/jut.log",
         "json":false
      },
      "dailyRotateFile":{
         "colorize":false,
         "timestamp":true,
         "filename":"/var/log/jut-rotated.log",
         "maxFiles": 5,
         "json":false
      },
      "syslog":{
         "host":"jx3-logs.jut.io",
         "port":514,
         "protocol":"udp4"
      }
   }
}
```

### DEBUG environment variable

For node.js processes, a shortcut is implemented to turn on debug
level logging by setting a list of prefixes in the `DEBUG` environment
variable.

For example the following would enable `search` and `autorequire`
debug logs but keep all other logs at info level:

    DEBUG=search,autorequire node foo.js

As a shortcut to enable all debug logs:

    DEBUG=* node bar.js
