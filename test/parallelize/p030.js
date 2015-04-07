#!/usr/local/bin/node --harmony

// parallelize -> persist -> filter (no args) -> reduce (no args)

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function isEven (e) {
		return (e % 2 == 0) ? true : false;
	}	

	function sum(a, b) {
		a += b;
		return a;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var tmp = v_copy.filter(isEven).reduce(sum, 0);

	var data = uc.parallelize(v).persist();
	yield data.reduce(sum, 0);

	v.push(6);
	var res = yield data.filter(isEven).reduce(sum, 0);

	console.assert(res == tmp);

	uc.end();
}).catch(ugrid.onError);
