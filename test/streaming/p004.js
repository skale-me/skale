#!/usr/local/bin/node --harmony

// stream -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

var s1 = fs.createReadStream('./f', {encoding: 'utf8'});
var s2 = fs.createReadStream('./f2', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var us1 = uc.stream(s1, {N: 4});

	uc.stream(s2, {N: 4}).union(us1).collect(function(err, res) {
		console.log('res: ' + res);
	});

	uc.jobs[0].stream.on('end', function() {
		uc.end();
	});

	// uc.end();
}).catch(ugrid.onError);
