#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var V = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

	function reducer(a, b) {return a + b;}

	var dist = yield uc.parallelize(V).reduce(reducer, 0);
	var local = V.reduce(reducer, 0);

	console.assert(dist == local);

	uc.end();
}).catch(ugrid.onError);
