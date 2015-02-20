#!/usr/local/bin/node --harmony

// Test randomSVMData -> map -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	function invertArray(v) {
		for (var i = 0; i < v.length; i++)
			v[i] = -v[i];
		return v;
	}

	var N = 5, D = 1, seed = 1;
	var ref = ml.randomSVMData(N, D, seed).map(invertArray).length;
	var res = yield ugrid.randomSVMData(N, D, seed).map(invertArray).count();
	console.assert(ref == res);

	ugrid.end();
})();
