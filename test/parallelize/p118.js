#!/usr/local/bin/node --harmony

// parallelize -> substract -> reduce
// parallelize -> 

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var substract = require('../ugrid-test.js').substract;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function sum(a, b) {
		return (a + b);
	}

	var v1 = [1, 2, 3];
	var v2 = [3, 4, 5];

	var loc = substract(v1, v2).reduce(sum, 0);

	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2);

	var dist = yield d1.substract(d2).reduce(sum, 0);

	console.assert(loc == dist);

	ugrid.end();
})();