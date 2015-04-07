#!/usr/local/bin/node --harmony

// parallelize -> filter -> reduce

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function isEven(e) {
		return (e % 2 == 0) ? true : false;
	}

	function sum(a, b) {
		a += b;
		return a;
	}

	var res = yield uc.parallelize(v).filter(isEven).reduce(sum, 0);

	console.assert(res == v.filter(isEven).reduce(sum, 0))

	uc.end();
}).catch(ugrid.onError);
