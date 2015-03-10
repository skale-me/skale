#!/usr/local/bin/node --harmony

// Test randomSVMData -> sample -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var N = 5, D = 2, seed = 1, frac = 0.1, key = -1, withReplacement = true;

	var ref = test.randomSVMData(N, D, seed);
	ref = test.sample(ref, ugrid.worker.length, withReplacement, frac, seed).filter(function (e) {return e[0] == key;});
	var res = yield ugrid.randomSVMData(N, D, seed).sample(withReplacement, frac).lookup(key);
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
