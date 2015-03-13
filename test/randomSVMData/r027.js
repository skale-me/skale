#!/usr/local/bin/node --harmony

// Test randomSVMData -> groupByKey -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var N = 5, D = 1, seed = 1, key = 1;
	var ref = test.randomSVMData(N, D, seed, ugrid.worker.length);
	var ref = test.groupByKey(ref).filter(function (e) {return e[0] == key;});

	var res = yield ugrid.randomSVMData(N, D, seed).groupByKey().lookup(key);
	console.assert(test.arrayFlatEqual(ref, res, 2));
	ugrid.end();
})();
