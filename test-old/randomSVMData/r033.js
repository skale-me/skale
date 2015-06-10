#!/usr/local/bin/node --harmony

// Test randomSVMData -> distinct -> collect

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1;

	var ref = test.randomSVMData(N, D, seed);
	ref = test.distinct(ref);
	var res = yield uc.randomSVMData(N, D, seed).distinct().collect();
	console.log(ref);
	console.log("--------");
	console.log(res);
	//console.assert(test.arrayEqual(ref.sort(), res.sort()));

	uc.end();
}).catch(ugrid.onError);
