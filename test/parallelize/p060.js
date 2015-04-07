#!/usr/local/bin/node --harmony

// parallelize -> groupByKey -> count();

var co = require('co');
var ugrid = require('../../');
var groupByKey = require('../ugrid-test.js').groupByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var loc = groupByKey(v).length;
	var dist = yield uc.parallelize(v).groupByKey().count();

	console.assert(loc == dist);

	uc.end();
}).catch(ugrid.onError);
