#!/usr/local/bin/node --harmony

// parallelize -> sortByKey -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [["z", 2], ["a", 1], ["t", 3]];

	var loc = v.sort();
	var dist = yield ugrid.parallelize(v).sortByKey().collect();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
