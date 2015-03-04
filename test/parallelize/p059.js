#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var sample = require('../ugrid-test.js').sample;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[1, 2], [3, 4], [5, 6], [7, 8]];
	var frac = 0.5;
	var seed = 1;
	var key = 1;
	var withReplacement = true;

	var loc = sample(v, ugrid.worker.length, withReplacement, frac, seed);
	var dist = yield ugrid.parallelize(v).sample(withReplacement, frac).lookup(key);

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

	ugrid.end();
})();
