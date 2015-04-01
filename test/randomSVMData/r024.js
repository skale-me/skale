#!/usr/local/bin/node --harmony

// Test randomSVMData -> groupByKey -> count

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed, uc.worker.length);
	var ref = test.groupByKey(ref);
	var res = yield uc.randomSVMData(N, D, seed).groupByKey().count();
	console.assert(ref.length == res);

	uc.end();
}).catch(ugrid.onError);
