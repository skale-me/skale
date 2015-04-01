#!/usr/local/bin/node --harmony

// parallelize -> persist -> groupByKey -> collect

var co = require('co');
var ugrid = require('../../');
var groupByKey = require('../ugrid-test.js').groupByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var loc = groupByKey(v);

	var data = uc.parallelize(v).persist();
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

	uc.end();
}).catch(ugrid.onError);
