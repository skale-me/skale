#!/usr/local/bin/node --harmony

// Test randomSVMData followed by lookup

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1, key = 1;
	var ref = test.randomSVMData(N, D, seed).filter(function (e) {
		return e[0] == key;
	});
	var res = yield uc.randomSVMData(N, D, seed).lookup(key);

	console.assert(test.arrayEqual(res.sort(), ref.sort()));

	uc.end();
}).catch(ugrid.onError);
