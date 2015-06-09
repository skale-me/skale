#!/usr/local/bin/node --harmony

// Test randomSVMData -> groupByKey -> reduce

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	// Cumulative sum of key and values
	function csum(a, b) {
		a[0] += b[0];
		for (var i = 0; i < b[1].length; i++)
			a[1][0] += b[1][i];
		return a;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed, uc.worker.length);
	var ref = test.groupByKey(ref).reduce(csum, [0, [0]]);

	var res = yield uc.randomSVMData(N, D, seed).groupByKey().reduce(csum, [0, [0]]);

	console.assert(test.arrayEqual(ref, res));

	uc.end();
}).catch(ugrid.onError);
