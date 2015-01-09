#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var V = [1, 2, 3, 4, 5];
	var P = process.argv[2];

	var res = yield ugrid.parallelize(V, P).collect();

	if (V.length != res.length)
		throw 'error: local and distributed array have different lengths';

	for (var i = 0; i < V.length; i++)
		if (V[i] != res[i])
			throw 'error: local and distributed array have different elements';

	ugrid.end();
})();
