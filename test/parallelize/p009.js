#!/usr/local/bin/node --harmony

// parallelize -> map -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function by2(e) {
		return 2 * e;
	}

	var loc = v.map(by2);
	var dist = yield ugrid.parallelize(v).map(by2).collect();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
})();
