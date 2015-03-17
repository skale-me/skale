#!/usr/local/bin/node --harmony

// parallelize -> intersection -> reduce
// parallelize -> 

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var intersection = require('../ugrid-test.js').intersection;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function sum(a, b) {
		return (a + b);
	}

	var v1 = [1, 2, 3];
	var v2 = [3, 4, 5, 2];

	var loc = intersection(v1, v2).reduce(sum, 0);
	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2);

	var dist = yield d1.intersection(d2).reduce(sum, 0);

	console.assert(loc == dist);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
