#!/usr/local/bin/node --harmony

// parallelize -> map -> count

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function by2(e) {
		return 2 * e;
	}

	var dist = yield uc.parallelize(v).map(by2).count();

	console.assert(dist == v.length)

	uc.end();
}).catch(ugrid.onError);
