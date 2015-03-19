#!/usr/local/bin/node --harmony

// paralelize --> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var key = 1;
	var v = [[1, 1], [1, 2], [2, 3], [2, 4], [3, 5]];

	var loc = v.filter(function (e) {return (e[0] == key)});
	var dist = yield ugrid.parallelize(v).lookup(key);

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
