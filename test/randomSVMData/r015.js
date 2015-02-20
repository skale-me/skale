#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

function arrayEqual(a1, a2) {
	return JSON.stringify(a1) == JSON.stringify(a2);
}

co(function *() {
	yield ugrid.init();

	function dup(e) { return [e, e]; }

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 2, seed = 1, key = 1;
	var ref = ml.randomSVMData(N, D, seed);
	ref = ref.concat(ref).filter(function (e) {
		return e[0] == key;
	});

	var res = yield ugrid.randomSVMData(N, D, seed).flatMap(dup).lookup(key);

	console.assert(arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
