# winston-rotate

## Overview

Winston's default [rotating file transport](https://github.com/flatiron/winston/blob/master/lib/winston/transports/daily-rotate-file.js) has some shortcomings, namely:

0. It creates new logs daily, or if the file exceeds a max size, but it does not expunge old logs. This means that an additional module must be used to delete old log files.
1. It appends a date to the end of the log, including the active log. This can make it difficult to determine which file is the active log file.
2. It does not perform compression.

These shortcomings were the impetus for writing our own. `winston-rotate` uses [logrotate-stream](https://www.npmjs.org/package/logrotate-stream) under the covers, which is used to perform compression/rotation. We use winston's `common.log` to get timestamps, colorization, and log formatting.

## Options

| Field        | Required           | Description  |
| ------------- |-------------- | ----------------------------------- |
| file      | yes |  The filename of the logfile to write output to. |
| colorize      | no | Boolean flag indicating if we should colorize output.|
| timestamp | no | Boolean flag indicating if we should prepend output with timestamps (default false). If function is specified, its return value will be used instead of timestamps. |
| json | no | If true, messages will be logged as JSON (default true). |
| size | no | The max file size of a log before rotation occurs. Supports 1024, 1k, 1m, 1g. Defaults to 100m |
| keep | no |The number of rotated log files to keep (including the primary log file). Defaults to 5 |
| compress | no | Optionally compress rotated files with gzip. Defaults to true. |

## Usage

Include the following in a [logger-config's](https://github.com/jut-io/product/tree/master/src/lib/logger#logger-configjson) transport object:

```javascript
{
    rotate: {
        file: '/tmp/jut.log',
        colorize: false,
        timestamp: true,
        json: false,
        max: '100m',
        keep: 5,
        compress: true
    }
}

```