#!/usr/local/bin/node --harmony

// Test parallelize --> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function sum(a, b) {
		a += b;
		return a;
	}

	var res = yield ugrid.parallelize(v).reduce(sum, 0);

	console.assert(res == v.reduce(sum, 0));

	ugrid.end();
})();
