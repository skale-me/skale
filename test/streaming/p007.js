#!/usr/local/bin/node --harmony

// stream -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

var s1 = fs.createReadStream('./f', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var s = uc.stream(s1, {N: 4}).collect({stream: true});

	s.pipe(process.stdout);
	s.on('end', function () {
		uc.end();
	});
}).catch(ugrid.onError);
