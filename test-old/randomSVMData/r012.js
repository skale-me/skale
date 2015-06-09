#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> count

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function dup(e) {
		return [e, e];
	}

	function positiveLabel(v) { return v[0] > 0; }

	var N = 5, D = 2, seed = 1;
	var ref = test.randomSVMData(N, D, seed).length;
	var res = yield uc.randomSVMData(N, D, seed).flatMap(dup).count();
	console.assert( 2 * ref == res);

	uc.end();
}).catch(ugrid.onError);
