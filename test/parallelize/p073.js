#!/usr/local/bin/node --harmony

// parallelize -> persist -> groupByKey -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var groupByKey = require('..//ugrid-test.js').groupByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var loc = groupByKey(v);

	var data = ugrid.parallelize(v).persist();
	yield data.count();

	v.push([0, 11]);
	var dist = yield data.groupByKey().collect();

	loc = loc.sort();
	dist = dist.sort();
	for (var i = 0; i < loc.length; i++) {
		loc[i][1] = loc[i][1].sort();
		dist[i][1] = dist[i][1].sort();
		console.log(loc[i])
		console.log(dist[i])
	}

	console.assert(JSON.stringify(loc) == JSON.stringify(dist))

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
