#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

function arrayEqual(a1, a2) {
	return JSON.stringify(a1) == JSON.stringify(a2);
}

co(function *() {
	yield ugrid.init();

	function positiveLabel(v) {
		return v[0] > 0;
	}

	var N = 5, D = 1, seed = 1;
	var ref = ml.randomSVMData(N, D, seed).filter(positiveLabel);
	var res = yield ugrid.randomSVMData(N, D, seed).filter(positiveLabel).collect();
	console.assert(arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
