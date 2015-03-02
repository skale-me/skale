#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var frac = 0.1;

	var res = yield ugrid.parallelize(v).sample(frac).count();

	var P = ugrid.worker.length;
	// recreate partitions
	var part = {};
	for (var p = 0; p < P; p++)
		part[p] = []

	var p = 0;
	for (var i = 0; i < v.length; i++) {
		part[p].push(v[i]);
		p = (p + 1) % P;
	}

	var acc = 0;
	for (var p in part)
		acc += Math.ceil(part[p].length * frac)

	console.log(res)
	console.log(acc)

	console.assert(res == acc)

	ugrid.end();
})();
