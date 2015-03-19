#!/usr/local/bin/node --harmony

// Test randomSVMData -> mapValues -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function x2(v) {
		return v * 2;
	}

	function sum(a, b) {
		a[1] += b[1];
		return a;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed);

	for (var i = 0; i < ref.length; i++)
		ref[i][1] = x2(ref[i][1]);

	ref = ref.reduce(sum, [0, 0]);

	var res = yield ugrid.randomSVMData(N, D, seed).mapValues(x2).reduce(sum, [0, 0]);
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
