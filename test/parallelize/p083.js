#!/usr/local/bin/node --harmony

// parallelize -> sample -> persist -> lookup

var co = require('co');
var ugrid = require('../../');
var sample = require('../ugrid-test.js').sample;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4], [2, 5]];
	var frac = 0.5;
	var seed = 1;
	var key = 0;
	var withReplacement = true;

	var loc = sample(v, uc.worker.length, withReplacement, frac, seed).filter(function (e) {return (e[0] == key)});

	var data = uc.parallelize(v).sample(withReplacement, frac).persist();
	yield data.count();

	v.push([key, 11]);
	var dist = yield data.lookup(key);

	loc = loc.sort();
	dist = dist.sort();	
	for (var i = 0; i < loc.length; i++)
		for (var j = 0; j < loc[i].length; j++)
			console.assert(loc[i][j] == dist[i][j]);

	uc.end();
}).catch(ugrid.onError);
