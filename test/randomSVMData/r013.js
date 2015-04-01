#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> collect

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function dup(e) { return [e, e]; }

	var N = 5, D = 2, seed = 1;
	var ref = test.randomSVMData(N, D, seed);
	ref = ref.concat(ref);

	var res = yield uc.randomSVMData(N, D, seed).flatMap(dup).collect();

	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	uc.end();
}).catch(ugrid.onError);
