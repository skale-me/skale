#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> count

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1, frac = 0.1, withReplacement = true;

	var ref = test.randomSVMData(N, D, seed);
	ref = test.sample(ref, uc.worker.length, withReplacement, frac, seed);
	var res = yield uc.randomSVMData(N, D, seed).sample(withReplacement, frac).count();
	console.assert(ref.length == res);

	uc.end();
}).catch(ugrid.onError);
