#!/usr/local/bin/node --harmony

// parallelize -> distinct -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var distinct = require('../ugrid-test.js').distinct;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [3, 2, 1, 3];

	var loc = distinct(v);
	var dist = yield ugrid.parallelize(v).distinct().collect();

	loc = loc.sort();
	dist= dist.sort();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	ugrid.end();
})();
