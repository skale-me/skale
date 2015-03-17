#!/usr/local/bin/node --harmony

// parallelize -> countByValue

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var countByValue = require('../ugrid-test.js').countByValue;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 3];
	var loc = countByValue(v);
	var dist = yield ugrid.parallelize(v).countByValue();

	console.log(loc);
	console.log(dist)

	loc = loc.sort();
	dist = dist.sort();

	// console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
