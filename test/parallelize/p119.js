#!/usr/local/bin/node --harmony

// parallelize -> subtract -> lookup
// parallelize -> 

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var subtract = require('../ugrid-test.js').subtract;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var key = 1;
	var v1 = [[1, 2], [3, 4]];
	var v2 = [[3, 4], [5, 6]];

	var loc = subtract(v1, v2).filter(function(e) {return (e[0] == key)});

	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2);

	var dist = yield d1.subtract(d2).lookup(key);

	loc = loc.sort();
	dist = dist.sort();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
