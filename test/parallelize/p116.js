#!/usr/local/bin/node --harmony

// parallelize -> subtract -> count
// parallelize -> 

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var subtract = require('../ugrid-test.js').subtract;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v1 = [1, 2, 3];
	var v2 = [3, 4, 5];

	var loc = subtract(v1, v2);

	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2);

	var dist = yield d1.subtract(d2).count();

	console.assert(loc.length == dist)

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
