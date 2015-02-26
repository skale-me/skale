#!/usr/local/bin/node --harmony

// Test randomSVMData -> reduceByKey -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function sum(a, b) {
		a += b;
		return a;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed, ugrid.worker.length);
	var ref = test.reduceByKey(ref, sum, [0,0]);

	var res = yield ugrid.randomSVMData(N, D, seed).reduceByKey(sum, [0, 0]).collect();

	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
