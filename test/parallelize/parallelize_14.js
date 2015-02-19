#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	function by2 (e) {
		return e * 2;
	}

	function sum(a, b) {
		a += b;
		return a;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var tmp = v_copy.map(by2).reduce(sum, 0);

	var data = ugrid.parallelize(v).persist();
	yield data.reduce(sum, 0);

	v.push(6);
	var res = yield data.map(by2).reduce(sum, 0);

	assert(res == tmp);

	ugrid.end();
})();
