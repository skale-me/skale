#!/usr/local/bin/node --harmony

// Test parallelize -> map -> collect

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function by2(e) {
		return 2 * e;
	}

	var res = yield ugrid.parallelize(v).map(by2).collect();
	var res_sort = res.sort();

	var tmp_sort = v.map(function(e) {return e * 2}).sort();

	for (var i = 0; i < v.length; i++)
		assert(res_sort[i] == tmp_sort[i])

	ugrid.end();
})();
