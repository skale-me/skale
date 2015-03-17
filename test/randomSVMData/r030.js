#!/usr/local/bin/node --harmony

// Test randomSVMData -> reduceByKey -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function sum(a, b) {
		return a + b;
	}

	// Cumulative sum of key and values
	function csum(a, b) {
		a[0] += b[0];
		a[1] += b[1];
		return a;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed, ugrid.worker.length);
	var ref = test.reduceByKey(ref, sum, 0).reduce(csum, [0, 0]);

	var res = yield ugrid.randomSVMData(N, D, seed).reduceByKey(sum, 0).reduce(csum, [0, 0]);

	console.assert(test.arrayEqual(ref, res));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
