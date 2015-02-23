#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

co(function *() {
	yield ugrid.init();

	function dup(e) {
		return [e, e];
	}

	function positiveLabel(v) { return v[0] > 0; }

	var N = 5, D = 2, seed = 1;
	var ref = test.randomSVMData(N, D, seed).length;
	var res = yield ugrid.randomSVMData(N, D, seed).flatMap(dup).count();
	console.assert( 2 * ref == res);

	ugrid.end();
})();
