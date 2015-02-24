#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

co(function *() {
	yield ugrid.init();

	function dup(e) { return [e, e]; }

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 2, seed = 1;
	var ref = test.randomSVMData(N, D, seed);
	ref = ref.concat(ref).reduce(arraySum);

	var res = yield ugrid.randomSVMData(N, D, seed).flatMap(dup).reduce(arraySum, [0, 0, 0]);

	console.assert(test.arrayEqual(ref, res));

	ugrid.end();
})();
