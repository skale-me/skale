#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

co(function *() {
	yield ugrid.init();

	function positiveLabel(v) {
		return v[0] > 0;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed).filter(positiveLabel);
	var res = yield ugrid.randomSVMData(N, D, seed).filter(positiveLabel).collect();
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
