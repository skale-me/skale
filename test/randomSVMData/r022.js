#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 2, seed = 1, frac = 0.1;

	var ref = test.randomSVMData(N, D, seed);
	ref = test.sample(ref, ugrid.worker.length, frac, seed).reduce(arraySum, [0, 0, 0]);
	var res = yield ugrid.randomSVMData(N, D, seed).sample(frac).reduce(arraySum, [0, 0, 0]);
	console.assert(test.arrayEqual(ref, res));

	ugrid.end();
})();
