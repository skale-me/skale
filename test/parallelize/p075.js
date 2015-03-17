#!/usr/local/bin/node --harmony

// parallelize -> persist -> groupByKey -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var groupByKey = require('..//ugrid-test.js').groupByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var key = 0;
	var loc = groupByKey(v).filter(function (e) {return (e[0] == key)});

	var data = ugrid.parallelize(v).persist();
	yield data.count();

	v.push([key, 11]);
	var dist = yield data.groupByKey().lookup(key);

	console.assert(loc[0][0] == dist[0][0])
	for (var i = 0; i < loc[0][1].length; i++)
		console.assert(loc[0][1][i] == dist[0][1][i])

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
