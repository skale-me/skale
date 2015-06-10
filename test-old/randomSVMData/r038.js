#!/usr/local/bin/node --harmony

// Test randomSVMData -> flatMapValues -> reduce

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 2, seed = 1;

	function arraySum (x, y) {
		var res = [];
		for (var i = 0; i < x.length; i++)
			res[i] = x[i] + y[i];
		return res;
	}

	function mapper(e) {
		var out = [];
		for (var i = e; i <= 5; i++)
			out.push(i);
		return out;
	}

	var ref = test.randomSVMData(N, D, seed);
	ref = test.flatMapValues(ref, mapper).reduce(arraySum, [0, 0]);
	var res = yield uc.randomSVMData(N, D, seed).flatMapValues(mapper).reduce(arraySum, [0, 0]);
	console.assert(test.arrayEqual(ref, res));

	uc.end();
}).catch(ugrid.onError);
