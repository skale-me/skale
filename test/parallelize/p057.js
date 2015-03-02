#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');
var test = require('../ugrid-test.js');

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var frac = 0.1;
	var seed = 1;

	var r1 = yield ugrid.parallelize(v).sample(frac).collect();

	tmp = test.sample(v, ugrid.worker.length, frac, seed);

	r1 = r1.sort();
	tmp = tmp.sort();

	console.assert(r1.length == tmp.length);
	for (var i = 0; i < r1.length; i++)
		console.assert(r1[i] == tmp[i]);

	ugrid.end();
})();
