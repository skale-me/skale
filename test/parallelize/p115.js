#!/usr/local/bin/node --harmony

// parallelize -> intersection -> lookup
// parallelize -> 

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var intersection = require('../ugrid-test.js').intersection;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var key = 1;
	var v1 = [[1, 2], [3, 4]];
	var v2 = [[1, 2], [3, 5]];

	var loc = intersection(v1, v2).filter(function(e) {return (e[0] == key)});
	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2);

	var dist = yield d1.intersection(d2).lookup(key);

	loc = loc.sort();
	dist = dist.sort();
	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
})();
