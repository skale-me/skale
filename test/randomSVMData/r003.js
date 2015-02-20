#!/usr/local/bin/node --harmony

// Test randomSVMData followed by lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

function arrayEqual(a1, a2) {
	return JSON.stringify(a1) == JSON.stringify(a2);
}

co(function *() {
	yield ugrid.init();

	var N = 5, D = 1, seed = 1, key = 1;
	var ref = ml.randomSVMData(N, D, seed).filter(function (e) {
		return e[0] == key;
	});
	var res = yield ugrid.randomSVMData(N, D, seed).lookup(key);

	console.assert(arrayEqual(res.sort(), ref.sort()));

	ugrid.end();
})();
