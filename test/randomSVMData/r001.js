#!/usr/local/bin/node --harmony

// Test randomSVMData followed by collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

function arrayEqual(a1, a2) {
	return JSON.stringify(a1) == JSON.stringify(a2);
}

co(function *() {
	yield ugrid.init();

	var N = 5, D = 2, seed = 1;
	var ref = test.randomSVMData(N, D, seed);
	var res = yield ugrid.randomSVMData(N, D, seed).collect();

	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
