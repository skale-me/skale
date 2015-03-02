#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> persist -> collect();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var reduceByKey = require('..//ugrid-test.js').reduceByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	function reducer(a, b) {		
		a += b;
		return a;
	}

	var loc = reduceByKey(v, reducer, 0);

	var data = ugrid.parallelize(v).reduceByKey(reducer, 0).persist();
	yield data.count();

	v.push([0, 10]);
	var dist = yield data.collect();

	loc = loc.sort();
	dist = dist.sort();
	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		console.assert(loc[i][1] == dist[i][1]);
	}

	ugrid.end();
})();
