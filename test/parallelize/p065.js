#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> collect();

var co = require('co');
var ugrid = require('../../');
var reduceByKey = require('../ugrid-test.js').reduceByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	function reducer(a, b) {		
		a += b;
		return a;
	}

	var loc = reduceByKey(v, reducer, 0);
	var dist = yield uc.parallelize(v).reduceByKey(reducer, 0).collect();

	loc = loc.sort();
	dist = dist.sort();
	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		console.assert(loc[i][1] == dist[i][1]);
	}

	uc.end();
}).catch(ugrid.onError);
