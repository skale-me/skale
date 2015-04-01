#!/usr/local/bin/node --harmony

// parallelize -> mapValues -> count

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]];

	function by2(e) {
		return e * 2;
	}

	var res = yield uc.parallelize(v).mapValues(by2).count();

	console.assert(res == v.length)

	uc.end();
}).catch(ugrid.onError);
