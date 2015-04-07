#!/usr/local/bin/node --harmony

// parallelize -> distinct -> reduce

var co = require('co');
var ugrid = require('../../');
var distinct = require('../ugrid-test.js').distinct;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [3, 2, 1, 3];

	function sum(a, b) {
		return (a + b);
	}

	var loc = distinct(v).reduce(sum, 0);
	var dist = yield uc.parallelize(v).distinct().reduce(sum, 0);

	console.assert(loc == dist)

	uc.end();
}).catch(ugrid.onError);
