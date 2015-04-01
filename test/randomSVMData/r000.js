#!/usr/local/bin/node --harmony

// Test randomSVMData followed by count

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 2, seed = 1;
	var res = yield uc.randomSVMData(N, D, seed).count();
	console.assert(N == res);
	uc.end();
}).catch(ugrid.onError);
