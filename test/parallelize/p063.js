#!/usr/local/bin/node --harmony

// parallelize -> groupByKey -> lookup();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var groupByKey = require('..//ugrid-test.js').groupByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var key = 0;

	var loc = groupByKey(v).filter(function(e) {return (e[0] == key)});
	var dist = yield ugrid.parallelize(v).groupByKey().lookup(key);

	loc = loc.sort();
	dist = dist.sort();
	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		for (var j = 0; j < loc[i][1].length; j++)
			console.assert(loc[i][1][j] == dist[i][1][j])
	}

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
