#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> lookup

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

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 2, seed = 1, key = 1;
	var ref = ml.randomSVMData(N, D, seed).filter(positiveLabel).filter(function (e) {
		return e[0] == key;
	});
	var res = yield ugrid.randomSVMData(N, D, seed).filter(positiveLabel).lookup(key);
	console.assert(arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
