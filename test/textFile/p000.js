#!/usr/local/bin/node --harmony

// textFile -> count

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];
	var t0 = v.reduce(function(a, b) {return a + b + '\n'}, '');
	fs.writeFileSync('/tmp/v', t0);

	var dist = yield uc.textFile('/tmp/v').count();

	console.log(dist);

	console.assert(dist == v.length);

	uc.end();
}).catch(ugrid.onError);
