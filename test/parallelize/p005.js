#!/usr/local/bin/node --harmony

// parallelize -> persist -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var loc = JSON.parse(JSON.stringify(v));

	var data = ugrid.parallelize(v).persist();
	yield data.collect();
	v.push(6);
	var dist = yield data.collect();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
})();
