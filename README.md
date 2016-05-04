# skale-engine

[![Join the chat at https://gitter.im/skale-me/skale-engine](https://badges.gitter.im/skale-me/skale-engine.svg)](https://gitter.im/skale-me/skale-engine?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build
Status](https://travis-ci.org/skale-me/skale-engine.svg?branch=master)](https://travis-ci.org/skale-me/skale-engine)

High performance distributed data processing engine

Skale-engine is a fast and general purpose distributed data processing
system. It provides a high-level API in Javascript and an optimized
parallel execution engine on top of NodeJS.

Word count using skale:

```javascript
var sc = require('skale-engine').context();

sc.textFile('/path/...')
  .flatMap(line => line.split(' '))
  .map(word => [word, 1])
  .reduceByKey((a, b) => a + b, 0)
  .count().then(console.log);
```

## Features

* In-memory computing
* Controlled memory usage, spill to disk when necessary
* Fast multiple distributed streams
* realtime lazy compiling and running of execution graphs
* workers can connect through TCP or websockets
* very fast, see [benchmark](benchmark/)

## Docs & community

* [Documentation](https://skale-me.github.io/skale-engine)
* [Gitter](https://gitter.im/skale-me/skale-engine) for support and
  discussion
* [skale](https://groups.google.com/forum/#!forum/skale)
  mailing list for discussion about use and development

## Quickstart

The best and quickest way to get started with skale-engine is to use
[skale](https://www.npmjs.com/package/skale) to create, run
and deploy skale applications.

	$ sudo npm install -g skale  # Install skale command once and for all
	$ skale create my_app        # Create a new app, install skale-engine
	$ cd my_app
	$ skale run                  # Starts a local cluster if necessary and run

## Examples

In the following, we bypass [skale](https://www.npmjs.com/package/skale)
toolbelt, and use directly and only skale-engine. It's for you if you are
rather more interested by the skale-engine architecture, details and internals.

To run the internal examples, clone the skale-engine repository and
install the dependencies:

	$ git clone git://github.com/skale-me/skale-engine.git --depth 1
	$ cd skale-engine
	$ npm install

Then start a skale-engine server and workers on local host:

	$ npm start

Then run whichever example you want

	$ ./examples/wordcount.js /etc/hosts

## Tests

To run the test suite, first install the dependencies, then run `npm test`:

	$ npm install
	$ npm test

## People

The original authors of skale-engine are [Cedric Artigue](https://github.com/CedricArtigue) and [Marc Vertes](https://github.com/mvertes).

[List of all
contributors](https://github.com/skale-me/skale-engine/graphs/contributors)

## License

[Apache-2.0](LICENSE)
