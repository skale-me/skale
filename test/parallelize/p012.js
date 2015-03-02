#!/usr/local/bin/node --harmony

// parallelize -> filter (no args) -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];

	function isEven(e) {
		return (e % 2 == 0) ? true : false;
	}

	var res = yield ugrid.parallelize(v).filter(isEven).count();

	console.assert(res == v.filter(isEven).length)

	ugrid.end();
})();