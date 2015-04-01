#!/usr/local/bin/node --harmony

// parallelize -> groupByKey -> persist -> reduce (no args)

var co = require('co');
var ugrid = require('../../');
var groupByKey = require('../ugrid-test.js').groupByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function reducer(a, b) {
		a[0] += b[0];
		for (var i = 0; i < b[1].length; i++)
			a[1][0] += b[1][i];
		return a;
	}

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var loc = groupByKey(v).reduce(reducer, [0, [0]]);

	var data = uc.parallelize(v).groupByKey().persist();
	yield data.count();

	v.push([0, 11]);
	var dist = yield data.reduce(reducer, [0, [0]]);

	console.assert(loc[0] == dist[0]);
	console.assert(loc[1][0] == dist[1][0]);

	uc.end();
}).catch(ugrid.onError);
