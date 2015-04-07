#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var key = 1;
	var value = 2;
	var v = [[key, value], [3, 4], [5, 6]];

	function by2 (e) {
		e[1] *= 2;
		return e;
	}

	var data = uc.parallelize(v).map(by2).persist();
	yield data.lookup(key);

	v.push([key, value]);
	var res = yield data.lookup(key);

	console.assert(res.length == 1);
	console.assert(res[0][0] == key);
	console.assert(res[0][1] == value * 2);

	uc.end();
}).catch(ugrid.onError);
