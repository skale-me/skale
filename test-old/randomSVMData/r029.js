#!/usr/local/bin/node --harmony

// Test randomSVMData -> reduceByKey -> collect

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function sum(a, b) {
		return a + b;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed, uc.worker.length);
	var ref = test.reduceByKey(ref, sum, 0);

	var res = yield uc.randomSVMData(N, D, seed).reduceByKey(sum, 0).collect();

	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	uc.end();
}).catch(ugrid.onError);
