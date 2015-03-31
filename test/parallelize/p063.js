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

	console.assert(loc[0][0] == dist[0][0]);
	console.assert(JSON.stringify(loc[0][1].sort()) == JSON.stringify(dist[0][1].sort()))

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
