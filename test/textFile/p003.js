#!/usr/local/bin/node --harmony

// textFile --> map -> lookup

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var key = 1;
	var v = [[1, 1], [1, 2], [2, 3], [2, 4], [3, 5]];

	var loc = v.filter(function (e) {return (e[0] == key)});
	var t0 = v.reduce(function(a, b) {return a + (b[0] + ' ' + b[1]) + '\n'}, '');
	fs.writeFileSync('/tmp/v', t0);

	var dist = yield uc.textFile('/tmp/v').map(function(e) {return e.split(' ').map(parseFloat)}).lookup(key);

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	uc.end();
}).catch(ugrid.onError);
