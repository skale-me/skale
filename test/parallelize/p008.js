#!/usr/local/bin/node --harmony

// parallelize -> map -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function by2(e) {
		return 2 * e;
	}

	var dist = yield ugrid.parallelize(v).map(by2).count();

	console.assert(dist == v.length)

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
