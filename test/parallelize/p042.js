#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function by2 (e) {
		return e * 2;
	}

	function sum(a, b) {
		a += b;
		return a;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var tmp = v_copy.map(by2).reduce(sum, 0);

	var data = uc.parallelize(v).map(by2).persist();
	yield data.reduce(sum, 0);

	v.push(6);
	var res = yield data.reduce(sum, 0);

	console.assert(res == tmp);

	uc.end();
}).catch(ugrid.onError);
