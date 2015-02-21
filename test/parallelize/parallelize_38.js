#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]];

	function by2(e) {
		return e * 2;
	}

	function sum(a, b) {
		a[1] += b[1];
		return a;
	}

	var res = yield ugrid.parallelize(v).mapValues(by2).reduce(sum, [0, 0]);

	for (var i = 0; i < v.length; i++)
		v[i][1] = by2(v[i][1]);

	var tmp = v.reduce(sum, [0, 0]);

	console.assert(res[0] == tmp[0]);
	console.assert(res[1] == tmp[1]);

	ugrid.end();
})();
