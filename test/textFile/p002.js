#!/usr/local/bin/node --harmony

// textFile -> map -> reduce

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function sum(a, b) {
		return a + b;
	}

	var v = [1, 2, 3, 4, 5];
	var loc = v.reduce(sum, 0);
	var t0 = v.reduce(function(a, b) {return a + b + '\n'}, '');
	fs.writeFileSync('/tmp/v', t0);

	var dist = yield uc.textFile('/tmp/v').map(function(e) {return parseFloat(e)}).reduce(sum, 0);

	console.assert(dist == loc);

	uc.end();
}).catch(ugrid.onError);
