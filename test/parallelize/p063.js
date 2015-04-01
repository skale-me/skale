#!/usr/local/bin/node --harmony

// parallelize -> groupByKey -> lookup();

var co = require('co');
var ugrid = require('../../');
var groupByKey = require('../ugrid-test.js').groupByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var key = 0;

	var loc = groupByKey(v).filter(function(e) {return (e[0] == key)});
	var dist = yield uc.parallelize(v).groupByKey().lookup(key);

	console.assert(loc[0][0] == dist[0][0]);
	console.assert(JSON.stringify(loc[0][1].sort()) == JSON.stringify(dist[0][1].sort()))

	uc.end();
}).catch(ugrid.onError);
