#!/usr/local/bin/node --harmony

// parallelize --> reduce

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

	var dist = yield ugrid.parallelize(v).reduce(sum, 0);

	console.assert(dist == v.reduce(sum, 0));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
