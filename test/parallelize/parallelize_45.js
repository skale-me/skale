#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	function isEven (e) {
		return (e % 2 == 0) ? true : false;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var data = ugrid.parallelize(v).filter(isEven).persist();
	yield data.collect();

	v.push(6);
	var res = yield data.collect();

	res_sort = res.sort();
	tmp_sort = v_copy.filter(isEven).sort();

	console.assert(res_sort.length == tmp_sort.length);
	for (var i = 0; i < tmp_sort.length; i++)
		for (var j = 0; j < tmp_sort[i].length; j++)
			console.assert(tmp_sort[i][j] == res_sort[i][j])

	ugrid.end();
})();
