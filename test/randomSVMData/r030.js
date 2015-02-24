#!/usr/local/bin/node --harmony

// Test randomSVMData -> reduceByKey -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

co(function *() {
	yield ugrid.init();

	function sum(a, b) {
		a += b;
		return a;
	}

	// Cumulative sum of key and values
	function csum(a, b) {
		a[0] += b[0];
		for (var i = 0; i < b.length; i++)
			a[1] += b[i];
		return a;
	}

	function vsum(a, b) {
		a[0] += b[0];
		a[1] += b[1];
		return a;
	}

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed, ugrid.worker.length);
	console.log(ref);
	var ref = test.reduceByKey(ref, arraySum, [0, [0]]); // .reduce(csum, [0, [0]]);
	console.log(ref);

	var res = yield ugrid.randomSVMData(N, D, seed).reduceByKey(sum, [0, 0]).reduce(csum, [0, [0]]);
	console.log(res);

	console.assert(test.arrayEqual(ref, res));

	ugrid.end();
})();
