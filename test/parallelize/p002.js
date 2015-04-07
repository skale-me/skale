#!/usr/local/bin/node --harmony

// parallelize --> reduce

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function sum(a, b) {
		a += b;
		return a;
	}

	var dist = yield uc.parallelize(v).reduce(sum, 0);

	console.assert(dist == v.reduce(sum, 0));

	uc.end();
}).catch(ugrid.onError);
