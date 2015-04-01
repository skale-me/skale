#!/usr/local/bin/node --harmony

// parallelize -> sample -> persist -> reduce

var co = require('co');
var ugrid = require('../../');
var sample = require('../ugrid-test.js').sample;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];
	var frac = 0.1;
	var seed = 1;
	var withReplacement = true;

	function sum(a, b) {
		a += b;
		return a;
	}

	var loc = sample(v, uc.worker.length, withReplacement, frac, seed).reduce(sum, 0);

	var data = uc.parallelize(v).sample(withReplacement, frac).persist();
	yield data.count();

	v.push(6);
	var dist = yield data.reduce(sum, 0);

	console.assert(loc == dist);

	uc.end();
}).catch(ugrid.onError);
