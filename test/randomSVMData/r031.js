#!/usr/local/bin/node --harmony

// Test randomSVMData -> reduceByKey -> lookup

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function sum(a, b) {
		a += b;
		return a;
	}

	var N = 5, D = 1, seed = 1, key = 1;
	var ref = test.randomSVMData(N, D, seed, uc.worker.length);
	var ref = test.reduceByKey(ref, sum, 0).filter(function (e) {return e[0] == key;});

	var res = yield uc.randomSVMData(N, D, seed).reduceByKey(sum, 0).lookup(key);

	console.assert(test.arrayEqual(ref, res));

	uc.end();
}).catch(ugrid.onError);
