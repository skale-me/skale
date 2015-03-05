#!/usr/local/bin/node --harmony

// parallelize -> distinct -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var distinct = require('../ugrid-test.js').distinct;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [3, 2, 1, 3];

	function sum(a, b) {
		return (a + b);
	}

	var loc = distinct(v).reduce(sum, 0);
	var dist = yield ugrid.parallelize(v).distinct().reduce(sum, 0);

	console.assert(loc == dist)

	ugrid.end();
})();
