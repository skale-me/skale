#!/usr/local/bin/node --harmony

// Test parallelize -> persist -> map -> count

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	throw 'Borken: need to do parallelize partitioning lazyly'
	yield ugrid.init();

	var v = [[1, 2], [3, 4], [5, 6]];
	var data = ugrid.parallelize(v).persist();
	var res = yield data.count();

	v[0][1] = 10;
	var res = yield data.count();

	assert(v.length == res);

	ugrid.end();
})();
