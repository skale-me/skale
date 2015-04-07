#!/usr/local/bin/node --harmony

// parallelize -> subtract -> reduce
// parallelize -> 

var co = require('co');
var ugrid = require('../../');
var subtract = require('../ugrid-test.js').subtract;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function sum(a, b) {
		return (a + b);
	}

	var v1 = [1, 2, 3];
	var v2 = [3, 4, 5];

	var loc = subtract(v1, v2).reduce(sum, 0);

	var d1 = uc.parallelize(v1);
	var d2 = uc.parallelize(v2);

	var dist = yield d1.subtract(d2).reduce(sum, 0);

	console.assert(loc == dist);

	uc.end();
}).catch(ugrid.onError);
