#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> reduce();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var reduceByKey = require('..//ugrid-test.js').reduceByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	function reducerByKey(a, b) {
		a += b;
		return a;
	}

	// sum keys and values
	function reducer(a, b) {
		a[0] += b[0];
		a[1] += b[1];
		return a;
	}

	var loc = reduceByKey(v, reducerByKey, 0).reduce(reducer, [0, 0]);
	var dist = yield ugrid.parallelize(v).reduceByKey(reducerByKey, 0).reduce(reducer, [0, 0]);

	console.assert(loc[0] == dist[0]);
	console.assert(loc[1] == dist[1]);	

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
