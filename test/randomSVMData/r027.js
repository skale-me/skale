#!/usr/local/bin/node --harmony

// Test randomSVMData -> groupByKey -> lookup

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1, key = 1;
	var ref = test.randomSVMData(N, D, seed, uc.worker.length);
	var ref = test.groupByKey(ref).filter(function (e) {return e[0] == key;});

	var res = yield uc.randomSVMData(N, D, seed).groupByKey().lookup(key);
	console.assert(test.arrayFlatEqual(ref, res, 2));
	uc.end();
}).catch(ugrid.onError);
