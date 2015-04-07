#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]];

	function by2(e) {
		return e * 2;
	}

	function sum(a, b) {
		a[1] += b[1];
		return a;
	}

	var res = yield uc.parallelize(v).mapValues(by2).reduce(sum, [0, 0]);

	for (var i = 0; i < v.length; i++)
		v[i][1] = by2(v[i][1]);

	var tmp = v.reduce(sum, [0, 0]);

	console.assert(res[0] == tmp[0]);
	console.assert(res[1] == tmp[1]);

	uc.end();
}).catch(ugrid.onError);
