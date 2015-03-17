#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var key = 1;
	var value = 2;
	var v = [[key, value], [3, 4], [5, 6]];
	var v_copy = JSON.parse(JSON.stringify(v));

	function by2 (e) {
		return e * 2;
	}

	function sum(a, b) {
		a[1] += b[1];
		return a;
	}

	var data = ugrid.parallelize(v).persist();
	yield data.lookup(key);

	v.push([key, value]);
	var res = yield data.mapValues(by2).reduce(sum, [0, 0]);

	for (var i = 0; i < v_copy.length; i++)
		v_copy[i][1] = by2(v_copy[i][1]);

	var tmp = v_copy.reduce(sum, [0, 0]);

	console.assert(res[0] == tmp[0]);
	console.assert(res[1] == tmp[1]);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
