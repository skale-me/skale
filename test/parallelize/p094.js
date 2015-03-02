#!/usr/local/bin/node --harmony

// parallelize ------------> union -> collect
// parallelize -> persist ->

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var union = require('../ugrid-test.js').union;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v1 = [1, 2, 3, 4, 5];
	var v2 = [6, 7, 8, 9, 10];
	var loc = union(v1, v2);

	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2).persist();

	yield d2.count();
	v2.push(11);

	var dist = yield d1.union(d2).collect();

	loc = loc.sort();
	dist = dist.sort();
	for (var i = 0; i < loc.length; i++)
		console.assert(loc[i] == dist[i]);

	ugrid.end();
})();
