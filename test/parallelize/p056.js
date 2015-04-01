#!/usr/local/bin/node --harmony

// parallelize -> sample -> count

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

	var loc = sample(v, uc.worker.length, withReplacement, frac, seed);
	var dist = yield uc.parallelize(v).sample(withReplacement, frac).count();
	
	console.assert(dist == loc.length)

	uc.end();
}).catch(ugrid.onError);
