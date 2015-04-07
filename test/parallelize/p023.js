#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]];
	var key = 2;
	function by2(e) {
		return e * 2;
	}

	var res = yield uc.parallelize(v).mapValues(by2).lookup(key);

	for (var i = 0; i < v.length; i++)
		v[i][1] = by2(v[i][1]);

	var tmp = [];
	for (var i = 0; i < v.length; i++)
		if (v[i][0] == key)
			tmp.push(v[i]);

	res = res.sort();
	tmp = tmp.sort();

	for (var i = 0; i < tmp.length; i++) {
		console.assert(res[i][0] == tmp[i][0]);
		console.assert(res[i][1] == tmp[i][1]);
	}

	uc.end();
}).catch(ugrid.onError);
