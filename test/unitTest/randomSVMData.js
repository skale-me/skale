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

	console.log(res)

	assert(res.length == N);

	for (var i = 0; i < N; i++) {
		assert(res[i].label);
		assert(res[i].features);
		assert((res[i].label == 1) || (res[i].label == -1));
		assert(res[i].features.length == D);
	}

	var rng = new ml.Random(seed);
	var V = [];
	for (i = 0; i < N; i++)
		V.push({label: Math.round(Math.abs(rng.next())) * 2 - 1, features: rng.randn(D)});

	// for (i = 0; i < V.length; i++) {
	// 	assert(V[i].label == res[i].label);
	// 	for (var j = 0; j < V[i].features.length; j++) {
	// 		assert(V[i].features[j] == res[i].features[j]);
	// 	}
	// }

	ugrid.end();
})();
