#!/usr/local/bin/node --harmony

// Test randomSVMData -> map -> collect

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function invertArray(v) {
		for (var i = 0; i < v.length; i++)
			v[i] = -v[i];
		return v;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed).map(invertArray);
	var res = yield uc.randomSVMData(N, D, seed).map(invertArray).collect();
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	uc.end();
}).catch(ugrid.onError);
