#!/usr/local/bin/node --harmony

// parallelize -> persist -> sample -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var sample = require('../ugrid-test.js').sample;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4], [2, 5]];
	var frac = 0.5;
	var seed = 1;
	var key = 0;
	var withReplacement = true;

	var loc = sample(v, ugrid.worker.length, withReplacement, frac, seed).filter(function (e) {return (e[0] == key)});

	var data = ugrid.parallelize(v).persist();
	yield data.count();

	v.push([key, 11]);
	var dist = yield data.sample(withReplacement, frac).lookup(key);

	loc = loc.sort();
	dist = dist.sort();	
	for (var i = 0; i < loc.length; i++)
		for (var j = 0; j < loc[i].length; j++)
			console.assert(loc[i][j] == dist[i][j]);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
