#!/usr/local/bin/node --harmony

// Test randomSVMData -> map -> collect

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

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed).map(invertArray);
	var res = yield ugrid.randomSVMData(N, D, seed).map(invertArray).collect();
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
