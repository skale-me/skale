#!/usr/local/bin/node --harmony

var co = require('co');
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

	if (d1.length != n)
		throw 'error: bad number of elements returned'

	for (var i = 0; i < d1.length; i++)
		if ( V.indexOf(d1[i]) == -1)
			throw 'error: sampled data is not in array'

	ugrid.end();
})();

