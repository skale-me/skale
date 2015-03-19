#!/usr/local/bin/node --harmony

// parallelize -> join -> collect
// parallelize -> 

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var join = require('../ugrid-test.js').join;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v1 = [[0, 1], [1, 2], [2, 3], [2, 4], [5, 5]];
	var v2 = [[0, 5], [1, 6], [2, 7], [3, 8], [0, 9]];
	var loc = join(v1, v2);

	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2);

	var dist = yield d1.join(d2).collect();

	loc = loc.sort();
	dist = dist.sort();
	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		for (var j = 0; j < loc[i][1].length; j++)
			console.assert(loc[i][1][j] == dist[i][1][j]);
	}

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
