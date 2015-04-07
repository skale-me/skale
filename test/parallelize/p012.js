#!/usr/local/bin/node --harmony

// parallelize -> filter (no args) -> count

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function isEven(e) {
		return (e % 2 == 0) ? true : false;
	}

	var res = yield uc.parallelize(v).filter(isEven).count();

	console.assert(res == v.filter(isEven).length)

	uc.end();
}).catch(ugrid.onError);
