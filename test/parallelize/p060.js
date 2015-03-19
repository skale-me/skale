#!/usr/local/bin/node --harmony

// parallelize -> groupByKey -> count();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var groupByKey = require('..//ugrid-test.js').groupByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var loc = groupByKey(v).length;
	var dist = yield ugrid.parallelize(v).groupByKey().count();

	console.assert(loc == dist);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
