#!/usr/local/bin/node --harmony

// Test randomSVMData -> distinct -> reduce

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 2, seed = 1;

	var ref = test.randomSVMData(N, D, seed);
	ref = test.distinct(ref).reduce(arraySum, [0, 0, 0]);
	var res = yield uc.randomSVMData(N, D, seed).distinct().reduce(arraySum, [0, 0, 0]);
	console.assert(test.arrayEqual(ref, res));

	uc.end();
}).catch(ugrid.onError);
