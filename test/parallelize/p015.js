#!/usr/local/bin/node --harmony

// parallelize -> map -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var key = 1;
	var value = 2;
	var v = [[key, value], [3, 4], [5, 6]];

	function isValueEven (e) {
		return (e[1] % 2 == 0) ? true : false;
	}

	var res = yield ugrid.parallelize(v).filter(isValueEven).lookup(key);

	console.assert(res.length == 1);
	console.assert(res[0][0] == key);
	console.assert(res[0][1] == value);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
