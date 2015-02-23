#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

function arrayEqual(a1, a2) {
	return JSON.stringify(a1) == JSON.stringify(a2);
}

co(function *() {
	yield ugrid.init();

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 2, seed = 1, frac = 0.1;

	var ref = ml.randomSVMData(N, D, seed);
	ref = ml.sample(ref, frac, ugrid.worker.length).reduce(arraySum, [0, 0, 0]);
	var res = yield ugrid.randomSVMData(N, D, seed).sample(frac).reduce(arraySum, [0, 0, 0]);
	console.assert(arrayEqual(ref, res));

	ugrid.end();
})();
