#!/usr/local/bin/node --harmony

// Test randomSVMData -> distinct -> lookup

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1, key = -1;

	var ref = test.randomSVMData(N, D, seed);
	ref = test.distinct(ref).filter(function (e) {return e[0] == key;});
	var res = yield uc.randomSVMData(N, D, seed).distinct().lookup(key);
	console.assert(test.arrayEqual(ref, res));

	uc.end();
}).catch(ugrid.onError);
