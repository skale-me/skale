#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var V = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

	function reducer(a, b) {return a + b;}

	var dist = yield ugrid.parallelize(V).reduce(reducer, 0);
	var local = V.reduce(reducer, 0);

	assert(dist == local)

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
