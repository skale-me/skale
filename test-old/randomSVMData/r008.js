#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> count

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function positiveLabel(v) { return v[0] > 0; }

	var N = 5, D = 2, seed = 1;
	var ref = test.randomSVMData(N, D, seed).filter(positiveLabel).length;
	var res = yield uc.randomSVMData(N, D, seed).filter(positiveLabel).count();
	console.assert(ref == res);

	uc.end();
}).catch(ugrid.onError);
