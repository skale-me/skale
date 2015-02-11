#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	function doubles(n) {
		return n * 2;
	}

	var V = [1, 2, 3, 4, 5];
	var local = V.map(doubles);
	var dist = yield ugrid.parallelize(V).map(doubles).collect();

	assert(local.length == dist.length)

	for (var i = 0; i < local.length; i++)
		assert(local[i] == dist[i])

	ugrid.end();
})();
