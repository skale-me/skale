#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var a = '1 2 3 4 5';
	var b = '6 7 8 9 10';
	fs.writeFileSync('/tmp/persist.txt', a);

	var P = process.argv[2];
	var dist = ugrid.textFile('/tmp/persist.txt', P).persist();
	var res = yield dist.collect();
	fs.writeFileSync('/tmp/persist.txt', b);
	res = yield dist.collect();
	fs.unlink('/tmp/persist.txt');

	if (res[0] != a) throw 'error: persist()';

	ugrid.end();
})();
