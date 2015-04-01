#!/usr/local/bin/node --harmony

// parallelize -> persist -> map (no args) -> collect

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function by2 (e) {
		return e * 2;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var data = uc.parallelize(v).persist();
	yield data.collect();

	v.push(6);
	var res = yield data.map(by2).collect();

	res_sort = res.sort();
	tmp_sort = v_copy.map(by2).sort();

	for (var i = 0; i < tmp_sort.length; i++)
		for (var j = 0; j < tmp_sort[i].length; j++)
			console.assert(tmp_sort[i][j] == res_sort[i][j])

	uc.end();
}).catch(ugrid.onError);
