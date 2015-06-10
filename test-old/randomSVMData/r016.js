#!/usr/local/bin/node --harmony

// Test randomSVMData -> mapValues -> count

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function x2(v) {
		return v * 2;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed);
	var res = yield uc.randomSVMData(N, D, seed).mapValues(x2).count();
	console.assert(ref.length == res);

	uc.end();
}).catch(ugrid.onError);
