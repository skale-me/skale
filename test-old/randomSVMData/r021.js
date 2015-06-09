#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> collect

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 2, seed = 1, frac = 0.1, replace = false;

	var ref = test.randomSVMData(N, D, seed, uc.worker.length);
	ref = test.sample(ref, uc.worker.length, replace, frac, seed);
	var res = yield uc.randomSVMData(N, D, seed).sample(replace, frac, seed).collect();
	console.assert(ref.length == res.length);

	uc.end();
}).catch(ugrid.onError);
