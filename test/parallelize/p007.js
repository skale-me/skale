#!/usr/local/bin/node --harmony

// parallelize -> persist -> lookup

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var key = 1;
	var value = 2;	
	var v = [[key, value], [3, 4], [5, 6]];
	var loc = v.filter(function(e) {return (e[0] == key)});

	var data = ugrid.parallelize(v).persist();
	yield data.lookup(key);
	v.push([key, value]);
	var dist = yield data.lookup(key);

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
