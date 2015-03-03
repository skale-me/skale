#!/usr/local/bin/node --harmony

// parallelize -> persist -> crossProduct -> count
// parallelize -> 

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var crossProduct = require('../ugrid-test.js').crossProduct;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v1 = [1, 2, 3, 4, 5];
	var v2 = [6, 7, 8, 9, 10];
	var loc = crossProduct(v1, v2);

	var d1 = ugrid.parallelize(v1).persist();
	yield d1.count();
	v1.push(11);

	var d2 = ugrid.parallelize(v2);

	var dist = yield d1.crossProduct(d2).collect();

	loc = loc.sort();
	dist = dist.sort();

	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		for (var j = 0; j < loc[i][1].length; j++)
			console.assert(loc[i][1][j] == dist[i][1][j]);
	}

	ugrid.end();
})();
