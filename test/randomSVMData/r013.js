#!/usr/local/bin/node --harmony

// Test randomSVMData -> filter -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var test = require('../ugrid-test.js');

process.on('exit', function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function dup(e) { return [e, e]; }

	var N = 5, D = 2, seed = 1;
	var ref = test.randomSVMData(N, D, seed);
	ref = ref.concat(ref);

	var res = yield ugrid.randomSVMData(N, D, seed).flatMap(dup).collect();

	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
