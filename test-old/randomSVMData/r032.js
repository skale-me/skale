#!/usr/local/bin/node --harmony

// Test randomSVMData -> distinct -> count

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1;

	var ref = test.randomSVMData(N, D, seed, uc.worker.length);
	ref = test.distinct(ref);
	var res = yield uc.randomSVMData(N, D, seed).distinct().collect();
	console.assert(test.arrayFlatEqual(ref, res, 2));

	uc.end();
}).catch(ugrid.onError);
