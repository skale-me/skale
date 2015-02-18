#!/usr/local/bin/node --harmony

// Test parallelize -> persist -> map -> reduce

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	function by2 (e) {
		e[1] *= 2;
		return e;
	}	

	function sum(a, b) {
		a[1] += b[1];
		return a;
	}

	var v = [[1, 2], [3, 4], [5, 6]];
	var v_copy = JSON.parse(JSON.stringify(v));
	var tmp = v_copy.map(by2).reduce(sum, [0, 0]);

	var data = ugrid.parallelize(v).persist();
	yield data.reduce(sum, [0, 0]);

	v[0][1] = 10;
	var res = yield data.map(by2).reduce(sum, [0, 0]);

	assert(res[1] == tmp[1]);

	ugrid.end();
})();
