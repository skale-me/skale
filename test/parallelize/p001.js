#!/usr/local/bin/node --harmony

// parallelize --> collect

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];
	var dist = yield uc.parallelize(v).collect();

	console.assert(JSON.stringify(dist) == JSON.stringify(v));

	uc.end();
}).catch(ugrid.onError);
