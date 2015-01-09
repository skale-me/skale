#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	function doubles(n) {
		return n * 2;
	}

	var V = [1, 2, 3, 4, 5];
	var local = V.map(doubles);
	var dist = yield ugrid.parallelize(V).map(doubles, []).collect();

	if (local.length != dist.length)
		throw 'error: local and distributed array have different lengths';

	for (var i = 0; i < local.length; i++)
		if (local[i] != dist[i])
			throw 'error: local and distributed array have different elements';

	ugrid.end();
})();
