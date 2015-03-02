#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function dup (e) {
		return [e, e];
	}	

	function sum(a, b) {
		a += b;
		return a;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));

	var data = ugrid.parallelize(v).flatMap(dup).persist();
	yield data.reduce(sum, 0);

	v.push(6);
	var res = yield data.reduce(sum, 0);

	var tmp = v_copy.map(dup)
		.reduce(function (a, b) {return a.concat(b);}, [])
		.reduce(sum, 0);

	console.assert(res == tmp);

	ugrid.end();
})();
