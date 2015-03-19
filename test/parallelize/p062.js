#!/usr/local/bin/node --harmony

// parallelize -> groupByKey -> reduce();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var groupByKey = require('..//ugrid-test.js').groupByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	// Cumulative associative sum of keys and values of associated sequence
	function reducer(a, b) {
		a[0] += b[0];
		for (var i = 0; i < b[1].length; i++)
			a[1][0] += b[1][i];
		return a;
	}

	var loc = groupByKey(v).reduce(reducer, [0, [0]]);
	var dist = yield ugrid.parallelize(v).groupByKey().reduce(reducer, [0, [0]]);

	console.assert(loc[0] == dist[0]);
	console.assert(loc[1][0] == dist[1][0]);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
