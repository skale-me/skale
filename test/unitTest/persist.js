#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var a = '1 2 3 4 5';
	var b = '6 7 8 9 10';
	fs.writeFileSync('/tmp/persist.txt', a);

	var dist = ugrid.textFile('/tmp/persist.txt').persist();
	var res = yield dist.collect();
	fs.writeFileSync('/tmp/persist.txt', b);
	res = yield dist.collect();
	fs.unlink('/tmp/persist.txt');

	console.log(a)
	console.log(res)
	
	assert(res[0] == a);

	ugrid.end();
})();
