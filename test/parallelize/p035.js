#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var key = 1;
	var value = 2;
	var v = [[key, value], [3, 4], [5, 6]];

	function dup (e) {
		return [e, e];
	}

	var data = uc.parallelize(v).persist();
	yield data.lookup(key);

	v.push([key, value]);
	var res = yield data.flatMap(dup).lookup(key);

	var res = res.sort();

	assert(res.length == 2);
	assert(res[0][0] == key);
	assert(res[0][1] == value);
	assert(res[1][0] == key);
	assert(res[1][1] == value);

	uc.end();
}).catch(ugrid.onError);
