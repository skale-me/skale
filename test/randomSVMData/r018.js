#!/usr/local/bin/node --harmony

// Test randomSVMData -> mapValues -> reduce

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function x2(v) {
		return v * 2;
	}

	function sum(a, b) {
		a[1] += b[1];
		return a;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed);

	for (var i = 0; i < ref.length; i++)
		ref[i][1] = x2(ref[i][1]);

	ref = ref.reduce(sum, [0, 0]);

	var res = yield uc.randomSVMData(N, D, seed).mapValues(x2).reduce(sum, [0, 0]);
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	uc.end();
}).catch(ugrid.onError);
