#!/usr/local/bin/node --harmony

// textFile -> persist -> map -> reduce

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();
	
	function sum(a, b) {return a + b;}

	var v = [1, 2, 3, 4, 5];
	var loc = v.reduce(sum, 0);
	var t0 = v.reduce(function(a, b) {return a + b + '\n'}, '');
	fs.writeFileSync('/tmp/v', t0);

	var data = ugrid.textFile('/tmp/v').persist();
	yield data.collect();
	fs.writeFileSync('/tmp/v', '');

	var dist = yield data.map(function(e) {return parseFloat(e)}).reduce(sum, 0);

	console.assert(dist == loc);	

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
