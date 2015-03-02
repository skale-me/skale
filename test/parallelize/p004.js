#!/usr/local/bin/node --harmony

// parallelize -> persist -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var data = ugrid.parallelize(v).persist();
	yield data.count();

	v.push(6);
	var res = yield data.count();

	console.assert((v.length - 1) == res);

	ugrid.end();
})();
