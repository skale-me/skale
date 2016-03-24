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

## Installation
	npm install skale-engine

## Features

* In-memory computing
* Controlled memory usage, spill to disk when necessary
* Fast multiple distributed streams
* realtime lazy compiling and running of execution graphs
* workers can connect through TCP or websockets

## Docs & community

* [skale-engine API](doc/skale-API.md)
* [Gitter](https://gitter.im/skale-me/skale-engine) for support and
  discussions

## Quickstart

*IN PROGRESS*

The best and quickest way to get started with skale is to use
[skale-cli](https://github.com/skale-me/skale-cli) to create, test
and deploy skale applications.

## Examples

To run the examples, clone the skale-engine repository and
install the dependencies:

	$ git clone git://github.com/skale-me/skale-engine.git --depth 1
	$ cd skale-engine
	$ npm install

Then start a skale-engine server and workers on local host:

	$ npm start

Then run whichever example you want

	$ ./examples/core/wordcount.js /etc/hosts

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
