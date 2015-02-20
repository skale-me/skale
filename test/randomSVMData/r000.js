#!/usr/local/bin/node --harmony

// Test randomSVMData followed by count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var N = 5, D = 2, seed = 1;
	var res = yield ugrid.randomSVMData(N, D, seed).count();
	console.assert(N == res);

	ugrid.end();
})();
