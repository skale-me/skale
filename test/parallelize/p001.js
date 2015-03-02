#!/usr/local/bin/node --harmony

// Test parallelize followed by collect

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var res = yield ugrid.parallelize(v).collect();

	v_sort = v.sort();
	res_sort = res.sort();

	for (var i = 0; i < v_sort.length; i++)
		assert (v_sort[i] == res_sort[i])

	ugrid.end();
})();
