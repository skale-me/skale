#!/usr/local/bin/node --harmony

// parallelize -> flatMap -> count

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function dup(e) {
		return [e, e];
	}

	var res = yield uc.parallelize(v).flatMap(dup).count();

	console.assert(res == v.length * 2)

	uc.end();
}).catch(ugrid.onError);
