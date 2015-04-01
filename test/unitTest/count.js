#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var V = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

	var dist = yield uc.parallelize(V).count();

	assert(dist == V.length)

	uc.end();
}).catch(ugrid.onError);
