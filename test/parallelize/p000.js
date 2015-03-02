#!/usr/local/bin/node --harmony

// parallelize -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var res = yield ugrid.parallelize(v).count();

	console.assert(v.length == res);

	ugrid.end();
})();
