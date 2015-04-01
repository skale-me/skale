#!/usr/local/bin/node --harmony

// parallelize -> filter -> collect

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function isEven(e) {
		return (e % 2 == 0) ? true : false;
	}

	var res = yield uc.parallelize(v).filter(isEven).collect();
	var res_sort = res.sort();

	var tmp_sort = v.filter(isEven).sort();

	for (var i = 0; i < v.length; i++)
		console.assert(res_sort[i] == tmp_sort[i])

	uc.end();
}).catch(ugrid.onError);
