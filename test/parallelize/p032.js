#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function dup (e) {
		return [e, e];
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var data = ugrid.parallelize(v).persist();
	var res = yield data.count();

	v.push(6);
	var res = yield data.flatMap(dup).count();

	var tmp = v_copy.map(dup).reduce(function (a, b) {return a.concat(b);}, []);

	assert(tmp.length == res);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
