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
	var res = yield data.count();

	v.push(6);
	var res = yield data.count();

	var tmp = v_copy.filter(isEven);

	console.assert(tmp.length == res);

	ugrid.end();
})();
