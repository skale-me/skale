#!/usr/local/bin/node --harmony

// parallelize -> takeSample

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var withReplacement = false;
	var seed = 1;
	var dist = yield ugrid.parallelize(v).takeSample(withReplacement, 3, seed);

	console.log(dist);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
