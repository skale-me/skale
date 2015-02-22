#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var N = 5, D = 1, seed = 1, frac = 0.1;

	var ref = ml.randomSVMData(N, D, seed);
	ref = ml.sample(ref, frac, ugrid.worker.length);
	var res = yield ugrid.randomSVMData(N, D, seed).sample(frac).count();
	console.assert(ref.length == res);

	ugrid.end();
})();
