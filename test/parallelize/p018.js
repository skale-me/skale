#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function dup(e) {
		return [e, e];
	}

	function sum(a, b) {
		a += b;
		return a;
	}	

	var res = yield ugrid.parallelize(v).flatMap(dup).reduce(sum, 0);

	var tmp = v.map(dup).reduce(function(a, b) {return a.concat(b)}, []).reduce(sum, 0);
	
	assert(tmp == res);

	ugrid.end();
})();
