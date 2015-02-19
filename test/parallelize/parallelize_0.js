#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var res = yield ugrid.parallelize(v).count();

	assert(v.length == res);

	ugrid.end();
})();
