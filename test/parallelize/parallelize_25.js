#!/usr/local/bin/node --harmony

// Test parallelize -> map -> collect

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function dup(e) {
		return [e, e];
	}

	var res = yield ugrid.parallelize(v).flatMap(dup).collect();
	var res_sort = res.sort();

	console.log(res)

	// var tmp_sort = v.filter(isEven).sort();

	// for (var i = 0; i < v.length; i++)
	// 	assert(res_sort[i] == tmp_sort[i])

	ugrid.end();
})();
