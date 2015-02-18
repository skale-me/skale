#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var N = 5;
	var D = 2;
	var seed = 1;
	var res = yield ugrid.randomSVMData(N, D, seed).collect();

	assert(res.length == N);

	for (var i = 0; i < N; i++)
		assert(res[i].length == (D + 1));

	ugrid.end();
})();
