#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');
var sample = require('../ugrid-test.js').sample;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[1, 2], [3, 4], [5, 6], [7, 8]];
	var frac = 0.5;
	var seed = 1;
	var key = 1;
	var withReplacement = true;

	var loc = sample(v, uc.worker.length, withReplacement, frac, seed);
	var dist = yield uc.parallelize(v).sample(withReplacement, frac).lookup(key);

	// local lookup
	var tmp = [];
	for (var i = 0; i < loc.length; i++) {
		if (loc[i][0] == key)
			tmp.push(loc[i]);
	}

	console.assert(dist.length == tmp.length)
	for (var i = 0; i < dist.length; i++) {
		console.assert(dist[i][0] == tmp[i][0]);
		console.assert(dist[i][1] == tmp[i][1]);
	}

	uc.end();
}).catch(ugrid.onError);
