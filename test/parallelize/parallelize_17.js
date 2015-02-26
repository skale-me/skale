#!/usr/local/bin/node --harmony

// Test parallelize -> map -> collect

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function isEven(e) {
		return (e % 2 == 0) ? true : false;
	}

	var res = yield ugrid.parallelize(v).filter(isEven).collect();
	var res_sort = res.sort();

	var tmp_sort = v.filter(isEven).sort();

	for (var i = 0; i < v.length; i++)
		assert(res_sort[i] == tmp_sort[i])

	ugrid.end();
})();
