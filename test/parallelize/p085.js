#!/usr/local/bin/node --harmony

// parallelize -> groupByKey -> persist -> collect

var co = require('co');
var ugrid = require('../../');
var groupByKey = require('../ugrid-test.js').groupByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var loc = groupByKey(v);

	var data = uc.parallelize(v).groupByKey().persist();
	yield data.count();

	v.push([0, 11]);
	var dist = yield data.collect();

	loc.sort();
	dist.sort();

	for (var i = 0; i < loc.length; i++) {
		console.assert(loc[i][0] == dist[i][0]);
		console.assert(JSON.stringify(loc[i][1].sort()) == JSON.stringify(dist[i][1].sort()))
	}


	uc.end();
}).catch(ugrid.onError);
