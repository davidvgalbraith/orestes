# Orestes

Orestes is a scalable, searchable [timeseries database](https://en.wikipedia.org/wiki/Time_series_database) backed by [Apache Cassandra](http://cassandra.apache.org/) and [Elasticsearch](https://www.elastic.co/products/elasticsearch).

Getting started
---------------

First, you need Orestes:

`npm install orestes`

Orestes depends on Cassandra 2.2.4 and Elasticsearch 2.0.0. You can [download](http://www.apache.org/dyn/closer.lua/cassandra/2.2.4/apache-cassandra-2.2.4-bin.tar.gz) them [yourself](https://www.elastic.co/downloads/elasticsearch) or run `sh scripts/download-backends.sh`. Then you can [run](https://wiki.apache.org/cassandra/RunningCassandra) them [yourself](https://www.elastic.co/guide/en/elasticsearch/guide/current/running-elasticsearch.html) or run `sh scripts/run-backends.sh` if you downloaded them using `download-backends.sh`.

Once the backends are running, Orestes needs to know the address and port to connect to them. Orestes reads this information from the "cassandra" and "elasticsearch" nested objects in `conf/orestes-config.json`. The provided defaults will connect to Cassandra and Elasticsearch processes running on localhost with their default ports, as they do if you download and run them with `download-backends.sh` and `run-backends.sh`. If you want to connect to a cluster elsewhere, you'll have to change the address and host in `orestes-config.json`.

Once `orestes-config.json` is looking good, run `node lib/orestes.js`. If it's all wired up correctly, you'll soon see the message `Orestes is online!`.

Writing Data
------------

Orestes takes points to write via POST requests to the `/write` endpoint. The body of the POST request must be a JSON array of objects with a "time" field and a "value" field. Orestes will store these points. To wit:

```
curl -XPOST localhost:9668/write -H 'Content-Type: application/json' -d '[
{"time":"2015-11-17T23:36:08.308Z","value":17,"name":"test_series","some_tag":"one"},
{"time":"2015-11-17T23:36:08.309Z","value":57,"name":"test_series","some_tag":"two"},
{"time":"2015-11-17T23:36:08.310Z","value":93,"name":"test_series","some_tag":"two"}
]'
```
Under the hood, Orestes will split these points by "series". The series of a point is the key-value pairs of that point other than `time` and `value`. So the series in this data set are `{"name":"test_series","some_tag":"one"}` and `{"name":"test_series","some_tag":"two"}`. For each series, Orestes stores one document in Elasticsearch. Each series corresponds to a row in Cassandra that contains all the times and values in that series.

The response from Orestes is an object with a key called `errors` mapping to an array of objects describing any points that failed to write. For instance, let's try to write a point with no time field:
```
curl -XPOST localhost:9668/write -H 'Content-Type: application/json' -d '[
{"value":17,"name":"broken_point_no_time"}
]'

{
    "errors":[{
        "point":{"value":17,"name":"broken_point_no_time"},
        "error":"missing required keys: [\"time\"]"
    }]
}
```

Reading Data
------------
Orestes takes read requests via POSTs to the `/read` endpoint. The body of the POST should be an object defining the query. Possible keys for this object include:

#### start ####
The earliest timestamp to return points for, in UNIX milliseconds or ISO string format. If not specified, `start` defaults to UNIX 0 -- midnight on January 1, 1970.

#### end ####
The end of the query, in UNIX milliseconds or ISO string format. Points with exactly this timestamp will not be returned -- Orestes returns points inclusive of the start time but exclusive of the end time. If not specified, `end` defaults to the time Orestes receives the request.

#### query ####
A query in the [Elasticsearch query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) defining the series to return points from. If not specified, `query` defaults to `{match_all: {}}`, which will return all series.

#### aggregations ####
An array of aggregations to perform. Currently, the only supported aggregation is `[{"type": "count"}]`. If you send `{"aggregations": [{"type": "count"}]}` in your Orestes query, Orestes will return counts of points in the Orestes query's time range for series matching the `query`.

### Examples ###

Let's run some basic queries on the data we wrote earlier. We'll start with the simplest possible query: by giving no request body, Orestes will use all the defaults, returning everything in the database. Be careful with this one -- it might take a while if you have a lot of data!

```
curl -XPOST localhost:9668/read

{
    "series": [
        {
            "tags": {
                "name": "test_series",
                "some_tag": "one"
            },
            "points": [
                [
                    1447803368308,
                    17
                ]
            ]
        },
        {
            "tags": {
                "name": "test_series",
                "some_tag": "two"
            },
            "points": [
                [
                    1447803368309,
                    57
                ],
                [
                    1447803368310,
                    93
                ]
            ]
        }
    ]
}
```
There you can see the format of the response Orestes returns. The response has a key called "series" mapping to an array of objects representing the series that the query matched. Each one of these objects has a key called "tags", which consists of the key-value pairs defining the series, and a key called "points", which is an array of the data points in that series in the given time range. Each data point is represented as an array `[timestamp, value]`. There's one more key that is sometimes present -- that key is `error`. If the `error` key is present, it means that the query failed at some point. The response may also contain some results in the `series` key, because Orestes streams results over HTTP as soon as they are available. Any results in a response that also contains an `error` key should be considered partial.

Let's run a slightly more interesting query:

```
curl -XPOST localhost:9668/read -H 'Content-Type: application/json' -d '{"start": 1447803368308, "end": 1447803368310}'

{
    "series": [
        {
            "tags": {
                "name": "test_series",
                "some_tag": "one"
            },
            "points": [
                [
                    1447803368308,
                    17
                ]
            ]
        },
        {
            "tags": {
                "name": "test_series",
                "some_tag": "two"
            },
            "points": [
                [
                    1447803368309,
                    57
                ]
            ]
        }
    ]
}
```
Here, we specified `start` and `end`. As you can see, Orestes included the start time, but left out the end time. So the last point in the second series was not returned.

Now let's send a nontrivial Elasticsearch query:
```
curl -XPOST localhost:9668/read -H 'Content-Type: application/json' -d '{"query": {"term": {"some_tag": "one"}}}'

{
    "series": [
        {
            "tags": {
                "name": "test_series",
                "some_tag": "one"
            },
            "points": [
                [
                    1447803368308,
                    17
                ]
            ]
        }
    ]
}
```
The Elasticsearch query matched only our first series, so Orestes only returned that series.


Finally, let's do some counting:

```
curl -XPOST localhost:9668/read -H 'Content-Type: application/json' -d '{"aggregations": [{"type": "count"}]}'

{
    "series": [
        {
            "tags": {
                "name": "test_series",
                "some_tag": "one"
            },
            "count": 1
        },
        {
            "tags": {
                "name": "test_series",
                "some_tag": "two"
            },
            "count": 2
        }
    ]
}
```

### streaming ###
As alluded to above, Orestes streams results over HTTP as soon as they are available. So if you make a large query, it may be helpful to use a streaming JSON parser so you can process results in real time instead of waiting for the whole query to finish. Here's an example using the [Oboe.js](http://oboejs.com/) streaming JSON library:

```
oboe({
    method: 'POST',
    url: 'http://localhost:9668/read'
})
.node('series.*', function(series) {
    console.log('received series', series);
});

received series { tags: { name: 'test_series', some_tag: 'two' },
  points: [ [ 1447803368309, 57 ], [ 1447803368310, 93 ] ] }
received series { tags: { name: 'test_series', some_tag: 'one' },
    points: [ [ 1447803368308, 17 ] ] }
```

Reading series
--------------
Sometimes it is useful to perform metadata computations on only the tags of a series, not the points. Orestes has some APIs that facilitate doing this efficiently.

#### /series ####
The `/series` endpoint takes the same query format as the `/read` endpoint, and it returns the same result, minus the `points` key in the series objects. Let's take a look at some examples.

```
curl -XPOST localhost:9668/series

{
    "series": [
        {
            "name": "test_series",
            "some_tag": "one"
        },
        {
            "name": "test_series",
            "some_tag": "two"
        }
    ]
}

curl -XPOST localhost:9668/series -H 'Content-Type: application/json' -d '{"query": {"term": {"some_tag": "one"}}}'

{
    "series": [
        {
            "name": "test_series",
            "some_tag": "one"
        }
    ]
}
```

### select_distinct ###
Orestes also provides a fast API for determining the different values for a given key in the stored data. To access this data, send a POST to the `/select_distinct` endpoint. The body of the POST should be an object with a key called `keys` mapping to an array of keys you are interested in. Orestes will return the combinations of these keys in the stored data. An example:

```
curl -XPOST localhost:9668/select_distinct -H 'Content-Type: application/json' -d '{"keys": ["some_tag"]}'
[{"some_tag":"one"},{"some_tag":"two"}]

curl -XPOST localhost:9668/select_distinct -H 'Content-Type: application/json' -d '{"keys": ["some_tag", "name"]}'
[{"some_tag":"one","name":"test_series"},{"some_tag":"two","name":"test_series"}]
```

To run `select_distinct` queries on multiple keys, Elasticsearch needs to have the script `aggkey.groovy` from orestes/scripts copied into `elasticsearch-2.0.0/config/scripts`. This is handled by `download-backends.sh` if you chose that method of installation.

Performance
-----------
Orestes comes with a handy script called `scripts/perf-test.js` that you can use to evaluate its performance. This script writes and reads a specified number of points with a specified format. You specify the number and format with some command line options:

#### --num_points ####
The number of points to write. Defaults to 100,000.

#### --num_tags ####
The number of tags other than "time" and "value" to include on each point. Defaults to 3.

#### --num_values ####
The number of distinct values of each tag. Defaults to 10. This means that the number of series involved in a run of `perf-test.js` is `(num_values)^(num_tags)`.

The latter two parameters are critical for analyzing the performance of Orestes or any timeseries database, because it is faster to read points from the same series than to orchestrate a read from a different series. Orestes has some clever mechanisms for minimizing this cost, but large table scans can nonetheless be expensive. Here are a few representative runs of `perf-test.js` on a late-2013 Macbook Pro with a 2.4 GHz Intel Core i5 processor and 4 GB of 1600 MHz DDR3 memory, running Cassandra, Elasticsearch and Orestes all on localhost, to give a flavor of Orestes' performance and the effect of the structure of the written data:

```
node scripts/perf-test.js  --num_points 100000 --num_tags 1 --num_values 1
wrote 100000 points in 1.984 seconds
read 100000 points from 1 series in 0.547 seconds

node scripts/perf-test.js  --num_points 100000 --num_tags 1 --num_values 10
wrote 100000 points in 2.455 seconds
read 100000 points from 10 series in 0.446 seconds

node scripts/perf-test.js  --num_points 100000 --num_tags 2 --num_values 10
wrote 100000 points in 2.462 seconds
read 100000 points from 100 series in 0.74 seconds

node scripts/perf-test.js  --num_points 100000 --num_tags 3 --num_values 10
wrote 100000 points in 3.065 seconds
read 100000 points from 1000 series in 3.342 seconds

node scripts/perf-test.js  --num_points 100000 --num_tags 4 --num_values 10
wrote 100000 points in 3.548 seconds
read 100000 points from 10000 series in 30.634 seconds

node scripts/perf-test.js  --num_points 1000000 --num_tags 4 --num_values 10
wrote 1000000 points in 24.974 seconds
read 1000000 points from 10000 series in 33.09 seconds
```

If you can parse all that info (note the number of points jumps to a million in the last one), it basically means that the time it takes to write some points is linear in the number of points written and barely affected by the number of series, while read time is linear in the number of series and barely affected by the number of points. That's why Orestes uses Elasticsearch -- by having such an expressive API for filtering series, you can make sure your queries don't try to read from 10,000+ series, maximizing performance.

So that's Orestes. Give it a go, play around with the perf-test, and see if it works for your use case. Thanks!
