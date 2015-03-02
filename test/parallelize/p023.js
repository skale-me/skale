#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]];
	var key = 2;
	function by2(e) {
		return e * 2;
	}

	var res = yield ugrid.parallelize(v).mapValues(by2).lookup(key);

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

	ugrid.end();
})();
