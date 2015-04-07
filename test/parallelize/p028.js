#!/usr/local/bin/node --harmony

// parallelize -> persist -> filter (no args) -> count

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function isEven (e) {
		return (e % 2 == 0) ? true : false;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var data = uc.parallelize(v).persist();
	var res = yield data.count();

	v.push(6);
	var res = yield data.filter(isEven).count();

	var tmp = v_copy.filter(isEven);

	console.assert(tmp.length == res);

	uc.end();
}).catch(ugrid.onError);
