#!/usr/local/bin/node --harmony

// Test paralelize followed by lookup

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var v = [[1, 1], [1, 2], [2, 3], [2, 4], [3, 5]];
	var key = 1;
	var res = yield ugrid.parallelize(v).lookup(key);
	res_sort = res.sort();

	var tmp = v.filter(function (e) {return (e[0] == key) ? true : false});
	tmp_sort = tmp.sort();

	for (var i = 0; i < tmp_sort.length; i++)
		for (var j = 0; j < tmp_sort[i].length; j++)
			assert(tmp_sort[i][j] == res_sort[i][j])

	ugrid.end();
})();
