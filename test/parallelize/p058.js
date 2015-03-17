#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var sample = require('../ugrid-test.js').sample;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var frac = 0.5;
	var seed = 1;
	var withReplacement = true;

	function sum(a, b) {
		a += b;
		return a;
	}

	var loc = sample(v, ugrid.worker.length, withReplacement, frac, seed);
	var dist = yield ugrid.parallelize(v).sample(withReplacement, frac).reduce(sum, 0);

	console.assert(dist == loc.reduce(sum, 0));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
