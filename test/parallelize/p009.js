#!/usr/local/bin/node --harmony

// parallelize -> map -> collect

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function by2(e) {
		return 2 * e;
	}

	var loc = v.map(by2);
	var dist = yield uc.parallelize(v).map(by2).collect();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	uc.end();
}).catch(ugrid.onError);
