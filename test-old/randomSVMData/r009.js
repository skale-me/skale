#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> collect

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function positiveLabel(v) {
		return v[0] > 0;
	}

	var N = 5, D = 1, seed = 1;
	var ref = test.randomSVMData(N, D, seed).filter(positiveLabel);
	var res = yield uc.randomSVMData(N, D, seed).filter(positiveLabel).collect();
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	uc.end();
}).catch(ugrid.onError);
