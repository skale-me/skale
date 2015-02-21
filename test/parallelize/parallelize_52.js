#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var key = 1;
	var value = 2;
	var v = [[key, value], [3, 4], [5, 6]];
	var v_copy = JSON.parse(JSON.stringify(v));

	function by2 (e) {
		return e * 2;
	}

	var data = ugrid.parallelize(v).persist();
	yield data.lookup(key);

	v.push([key, value]);
	var res = yield data.mapValues(by2).count();

	for (var i = 0; i < v_copy.length; i++)
		v_copy[i][1] = by2(v_copy[i][1]);

	console.assert(v_copy.length == res);

	ugrid.end();
})();
