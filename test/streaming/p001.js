#!/usr/local/bin/node --harmony

// stream -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	uc.stream(process.stdin, {N: 3}).collect(function(err, res) {
		console.log('res: ' + res);
	});
	uc.stream(fs.createReadStream(process.argv[2], {encoding: 'utf8'}), {N: 3}).collect(function(err, res) {
		console.log('res: ' + res);
	});

	// uc.end();
}).catch(ugrid.onError);
