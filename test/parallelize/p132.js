#!/usr/local/bin/node --harmony

// parallelize -> partitionByKey -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [["z", 2], ["a", 1], ["t", 3]];

	var dist = yield ugrid.parallelize(v).partitionByKey().collect();

	console.log(dist)

	ugrid.end();
})();
