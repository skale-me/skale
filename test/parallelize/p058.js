#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');
var sample = require('../ugrid-test.js').sample;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];
	var frac = 0.5;
	var seed = 1;
	var withReplacement = true;

	function sum(a, b) {
		a += b;
		return a;
	}

	var loc = sample(v, uc.worker.length, withReplacement, frac, seed);
	var dist = yield uc.parallelize(v).sample(withReplacement, frac).reduce(sum, 0);

	console.assert(dist == loc.reduce(sum, 0));

	uc.end();
}).catch(ugrid.onError);
