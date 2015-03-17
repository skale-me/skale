#!/usr/local/bin/node --harmony

// textFile -> persist -> collect

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [1, 2, 3, 4, 5];
	var loc = v.map(String);

	var t0 = v.reduce(function(a, b) {return a + b + '\n'}, '');
	fs.writeFileSync('/tmp/v', t0);

	var data = ugrid.textFile('/tmp/v').persist();
	yield data.collect();
	fs.writeFileSync('/tmp/v', '');
	var dist = yield data.collect();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	ugrid.end();
})();