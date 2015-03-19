#!/usr/local/bin/node --harmony

// parallelize -> distinct -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var distinct = require('../ugrid-test.js').distinct;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[3, 1], [2, 1], [1, 4], [3, 1]];
	var key = 3;

	var loc = distinct(v).filter(function(e) {return (e[0] == key);});
	var dist = yield ugrid.parallelize(v).distinct().lookup(key);

	loc = loc.sort();
	dist = dist.sort();
	console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
