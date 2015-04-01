#!/usr/local/bin/node --harmony

// parallelize -> persist -> sample -> collect

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

	var data = uc.parallelize(v).persist();
	yield data.count();

	v.push(6);
	var dist = yield data.sample(withReplacement, frac).collect();

	loc = loc.sort();
	dist = dist.sort();

	console.assert(loc.length == dist.length);
	for (var i = 0; i < loc.length; i++)
		console.assert(loc[i] == dist[i]);

	uc.end();
}).catch(ugrid.onError);
