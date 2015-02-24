#!/usr/local/bin/node --harmony

// Test randomSVMData -> mapValues -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

co(function *() {
	yield ugrid.init();

	function x2(v) {
		return v * 2;
	}

	var N = 5, D = 1, seed = 1, key = 1;
	var ref = test.randomSVMData(N, D, seed);

	for (var i = 0; i < ref.length; i++)
		ref[i][1] = x2(ref[i][1]);

	ref = ref.filter(function (e) {return e[0] == key;});

	var res = yield ugrid.randomSVMData(N, D, seed).mapValues(x2).lookup(key);
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
