#!/usr/local/bin/node --harmony

// parallelize -> values -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[1, 2], [3, 4], [3, 6]];

	var loc = v.map(function(e){return e[1]});

	var dist = yield ugrid.parallelize(v).values().collect();

	loc = loc.sort();
	dist = dist.sort();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
