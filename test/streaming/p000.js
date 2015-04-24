#!/usr/local/bin/node --harmony

// stream -> collect

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	uc.stream(process.stdin, {N: 10}).collect(function(err, res) {
	//uc.stream({N: 10}).count(function(err, res) {
		console.log('res: ' + res);
	});

	// uc.end();
}).catch(ugrid.onError);
