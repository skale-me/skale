#!/usr/local/bin/node --harmony

// Test randomSVMData followed by reduce

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
	var ref = test.randomSVMData(N, D, seed).reduce(arraySum);
	var res = yield uc.randomSVMData(N, D, seed).reduce(arraySum, [0, 0, 0]);

	console.assert(test.arrayEqual(ref, res));

	uc.end();
}).catch(ugrid.onError);
