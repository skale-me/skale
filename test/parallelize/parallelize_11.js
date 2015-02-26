#!/usr/local/bin/node --harmony

// Test parallelize -> map -> lookup

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var key = 1;
	var value = 2;
	var v = [[key, value], [3, 4], [5, 6]];

	function by2 (e) {
		e[1] *= 2;
		return e;
	}

	var res = yield ugrid.parallelize(v).map(by2).lookup(key);

	assert(res.length == 1);
	assert(res[0][0] == key);
	assert(res[0][1] == value * 2);

	ugrid.end();
})();
