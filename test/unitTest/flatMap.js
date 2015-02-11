#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var dist = yield ugrid
		.parallelize([1, 2, 3, 4, 5])
		.flatMap(function expand(n) {
			return [n, n];
		})
		.collect();

	assert(dist[0] == 1);
	assert(dist[1] == 1);
	assert(dist[2] == 2);
	assert(dist[3] == 2);
	assert(dist[4] == 3);
	assert(dist[5] == 3);
	assert(dist[6] == 4);
	assert(dist[7] == 4);
	assert(dist[8] == 5);
	assert(dist[9] == 5);

	ugrid.end();
})();
