#!/usr/local/bin/node --harmony

// Test randomSVMData -> map -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function invertArray(v) {
		for (var i = 0; i < v.length; i++)
			v[i] = -v[i];
		return v;
	}

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	var N = 5, D = 2, seed = 1, key = 1;
	var ref = test.randomSVMData(N, D, seed).map(invertArray).filter(function (e) {
		return e[0] == key;
	});
	var res = yield ugrid.randomSVMData(N, D, seed).map(invertArray).lookup(key);
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
