#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var N = 10;
	var n = 2;
	var seed = 1;
	var V = [];

	for (var i = 0; i < N; i++)
		V[i] = i;

	var d1 = yield ugrid.parallelize(V).takeSample(n, seed);

	assert(d1.length == n)

	for (i = 0; i < d1.length; i++)
		assert(V.indexOf(d1[i]) != -1)

	ugrid.end();
})();

