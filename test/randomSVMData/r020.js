#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var N = 5, D = 1, seed = 1, frac = 0.1;

	var ref = test.randomSVMData(N, D, seed);
	ref = test.sample(ref, ugrid.worker.length, frac, seed);
	var res = yield ugrid.randomSVMData(N, D, seed).sample(frac).count();
	console.assert(ref.length == res);

	ugrid.end();
})();
