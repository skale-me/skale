#!/usr/local/bin/node --harmony

// textFile -> map -> reduce

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	function sum(a, b) {
		return a + b;
	}

	var v = [1, 2, 3, 4, 5];
	var loc = v.reduce(sum, 0);
	var t0 = v.reduce(function(a, b) {return a + b + '\n'}, '');
	fs.writeFileSync('/tmp/v', t0);

	var dist = yield ugrid.textFile('/tmp/v').map(function(e) {return parseFloat(e)}).reduce(sum, 0);

	console.assert(dist == loc);

	ugrid.end();
})();
