#!/usr/local/bin/node --harmony

// parallelize -> persist -> reduce

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();
	
	function sum(a, b) {
		a += b;
		return a;
	}

	var v = [1, 2, 3, 4, 5];

	var loc = v.reduce(sum, 0);
	var data = ugrid.parallelize(v).persist();
	yield data.reduce(sum, 0);
	v.push(6);
	var dist = yield data.reduce(sum, 0);

	console.assert(dist == loc);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
