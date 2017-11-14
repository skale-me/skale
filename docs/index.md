![logo](images/logo-skale.png)

[![Build
Status](https://travis-ci.org/skale-me/skale-engine.svg?branch=master)](https://travis-ci.org/skale-me/skale-engine)
[![Build Status](https://ci.appveyor.com/api/projects/status/github/skale-me/skale-engine?svg=true)](https://ci.appveyor.com/project/skaleme/skale-engine)


High performance distributed data processing and machine learning.

Skale provides a high-level API in Javascript and an optimized
parallel execution engine on top of NodeJS.

## Features
* Pure javascript implementation of a Spark like engine
* Multiple data sources: filesystems, databases, cloud (S3, azure)
* Multiple data formats: CSV, JSON, Columnar (Parquet)...
* 50 high level operators to build parallel apps
* Machine learning: scalable classification, regression, clusterization
* Run interactively in a nodeJS REPL shell
* Docker [ready](https://github.com/skale-me/skale-engine/blob/master/docker/), simple local mode or full distributed mode
* Very fast, see [benchmark](https://github.com/skale-me/skale-engine/blob/master/benchmark/)

## Quickstart
```sh
npm install skale
```

Word count example: 

```javascript
var sc = require('skale').context();

sc.textFile('/my/path/*.txt')
  .flatMap(line => line.split(' '))
  .map(word => [word, 1])
  .reduceByKey((a, b) => a + b, 0)
  .count(function (err, result) {
    console.log(result);
    sc.end();
  });
```

### Local mode
In local mode, worker processes are automatically forked and
communicate with app through child process IPC channel. This is
the simplest way to operate, and it allows to use all machine
available cores.

To run in local mode, just execute your app script:
```sh
node my_app.js
```

or with debug traces:
```sh
SKALE_DEBUG=2 node my_app.js
```

### Distributed mode
In distributed mode, a cluster server process and worker processes
must be started prior to start app. Processes communicate with each
other via raw TCP or via websockets.

To run in distributed cluster mode, first start a cluster server
on `server_host`:
```sh
./bin/server.js
```

On each worker host, start a worker controller process which connects
to server:
```sh
./bin/worker.js -H server_host
```

Then run your app, setting the cluster server host in environment:
```sh
SKALE_HOST=server_host node my_app.js
```

The same with debug traces:
```sh
SKALE_HOST=server_host SKALE_DEBUG=2 node my_app.js
```

## Resources

* [Contributing guide](https://github.com/skale-me/skale-engine/blob/master/CONTRIBUTING.md)
* [Gitter](https://gitter.im/skale-me/skale-engine) for support and
  discussion
* [Mailing list](https://groups.google.com/forum/#!forum/skale)
  for discussion about use and development

## Authors

The original authors of skale are [Cedric Artigue](https://github.com/CedricArtigue) and [Marc Vertes](https://github.com/mvertes).

[List of all
contributors](https://github.com/skale-me/skale-engine/graphs/contributors)

## License

[Apache-2.0](https://github.com/skale-me/skale-engine/blob/master/LICENSE)

## Credits

<div>Logo Icon made by <a href="https://www.flaticon.com/authors/smashicons" title="Smashicons">Smashicons</a> from <a href="https://www.flaticon.com/" title="Flaticon">www.flaticon.com</a> is licensed by <a href="http://creativecommons.org/licenses/by/3.0/" title="Creative Commons BY 3.0" target="_blank">CC 3.0 BY</a></div>
