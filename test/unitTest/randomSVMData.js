#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var N = 5;
	var D = 2;
	var seed = 1;
	var P = process.argv[2];
	var res = yield ugrid.randomSVMData(N, D, seed, P).collect();

	if (res.length != N)
		throw 'error: bad array length';

	for (var i = 0; i < N; i++) {
		if (res[i].label === undefined)
			throw 'error: no label found';
		if (res[i].features === undefined)
			throw 'error: no features found';
		if ((res[i].label != -1) && (res[i].label != 1))
			throw 'error: bad label value';
		if (res[i].features.length != D)
			throw 'error: bad features length';
	}

	var rng = new ml.Random(seed);
	var V = [];
	for (i = 0; i < N; i++)
		V.push({label: Math.round(Math.abs(rng.next())) * 2 - 1, features: rng.randn(D)});

	for (i = 0; i < V.length; i++) {
		if (V[i].label != res[i].label)
			throw 'error: different label in arrays';
		for (var j = 0; j < V[i].features.length; j++)
			if (V[i].features[j] != res[i].features[j])
				throw 'error: different features in arrays';
	}

	ugrid.end();
})();
