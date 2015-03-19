#!/usr/local/bin/node --harmony

// Test randomSVMData -> distinct -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var N = 5, D = 1, seed = 1;

	var ref = test.randomSVMData(N, D, seed, ugrid.worker.length);
	ref = test.distinct(ref);
	var res = yield ugrid.randomSVMData(N, D, seed).distinct().collect();
	console.assert(test.arrayFlatEqual(ref, res, 2));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
