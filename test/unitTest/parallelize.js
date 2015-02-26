#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var V = [1, 2, 3, 4, 5];

	var res = yield ugrid.parallelize(V).collect();

	console.log(res);
	assert(V.length == res.length);

	for (var i = 0; i < V.length; i++)
		assert (V[i] == res[i])

	ugrid.end();
})();
