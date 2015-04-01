#!/usr/local/bin/node --harmony

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
	var dist = yield uc.parallelize(v).sample(withReplacement, frac).collect();

	dist = dist.sort();
	loc = loc.sort();

	console.assert(dist.length == loc.length);
	for (var i = 0; i < dist.length; i++)
		console.assert(dist[i] == loc[i]);

	uc.end();
}).catch(ugrid.onError);
