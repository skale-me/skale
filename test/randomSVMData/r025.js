#!/usr/local/bin/node --harmony

// Test randomSVMData -> groupByKey -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

co(function *() {
	yield ugrid.init();

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed, ugrid.worker.length);
	var ref = test.groupByKey(ref);
	var res = yield ugrid.randomSVMData(N, D, seed).groupByKey().collect();
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
})();
