#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var N = 5, D = 2, seed = 1, frac = 0.1;

	var ref = test.randomSVMData(N, D, seed, ugrid.worker.length);
	//console.log(ref.sort());
	//ref = test.sample(ref, ugrid.worker.length, frac, seed);
	//ref = ml.sample(ref, frac, ugrid.worker.length, seed);
	console.log(ref.sort());
	//var res = yield ugrid.randomSVMData(N, D, seed).sample(frac).collect();
	var res = yield ugrid.randomSVMData(N, D, seed).collect();
	console.log(res.sort());
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
