#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	function by2 (e) {
		return e * 2;
	}

	var v = [1, 2, 3, 4, 5];
	var data = ugrid.parallelize(v).persist();
	var res = yield data.count();

	v.push(6);
	var res = yield data.map(by2).count();

	assert((v.length - 1) == res);

	ugrid.end();
})();
