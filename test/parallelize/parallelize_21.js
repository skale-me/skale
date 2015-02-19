#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	function isValueEven (e) {
		return (e[1] % 2 == 0) ? true : false;
	}

	var v = [[1, 2], [3, 4], [5, 6]];
	var v_copy = JSON.parse(JSON.stringify(v));
	var data = ugrid.parallelize(v).persist();
	yield data.collect();

	v[0][1] = 10;
	var res = yield data.filter(isValueEven).collect();

	res_sort = res.sort();
	tmp_sort = v_copy.filter(isValueEven).sort();

	assert(res_sort.length == tmp_sort.length);
	for (var i = 0; i < tmp_sort.length; i++)
		for (var j = 0; j < tmp_sort[i].length; j++)
			assert(tmp_sort[i][j] == res_sort[i][j])

	ugrid.end();
})();
