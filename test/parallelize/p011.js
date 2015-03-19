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
	var loc = JSON.parse(JSON.stringify(v)).map(by2).filter(function(e){return (e[0] == key)});

	function by2 (e) {
		e[1] *= 2;
		return e;
	}

	var dist = yield ugrid.parallelize(v).map(by2).lookup(key);

	console.log(loc)
	console.log(dist)

	console.assert(JSON.stringify(dist) == JSON.stringify(loc))

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
