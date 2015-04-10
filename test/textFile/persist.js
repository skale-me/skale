#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var assert = require('assert');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var a = '1 2 3 4 5';
	var b = '6 7 8 9 10';
	fs.writeFileSync('/tmp/persist.txt', a);

	var dist = uc.textFile('/tmp/persist.txt').persist();
	var res = yield dist.collect();
	fs.writeFileSync('/tmp/persist.txt', b);
	res = yield dist.collect();
	fs.unlink('/tmp/persist.txt');

	console.log(a)
	console.log(res)
	
	assert(res[0] == a);

	uc.end();
}).catch(ugrid.onError);
