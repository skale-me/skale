#!/usr/local/bin/node --harmony

// stream -> collect

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var us = uc.stream(process.stdin, {N: 3}).collectStream()
	
//	us.on('data', function(res) {
//		console.log('res: ' + res);
//	});
//
	us.on('end', function () {
		console.log("BYE");
		uc.end();
	});

	us.pipe(process.stdout);
}).catch(ugrid.onError);
