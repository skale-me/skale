#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function by2 (e) {
		return e * 2;
	}

	var v = [1, 2, 3, 4, 5];
	var data = uc.parallelize(v).map(by2).persist();
	var res = yield data.count();

	v.push(6);
	var res = yield data.count();

	console.assert((v.length - 1) == res);

	uc.end();
}).catch(ugrid.onError);
