#!/usr/local/bin/node --harmony

// Test parallelize -> persist -> lookup

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var key = 1;
	var value = 2;	
	var v = [[key, value], [3, 4], [5, 6]];

	var data = ugrid.parallelize(v).persist();
	yield data.lookup(key);

	v.push([key, value]);
	var res = yield data.lookup(key);

	assert(res.length == 1);
	assert(res[0][0] == key);
	assert(res[0][1] == value);	

	ugrid.end();
})();
