#!/usr/local/bin/node --harmony

// stream -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

var s1 = fs.createReadStream('./f', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var us1 = uc.stream(s1, {N: 4});

	uc.parallelize([5, 6, 7, 8]).union(us1).collect(function(err, res) {
		console.log('res: ' + res);
	});

	uc.jobs[0].stream.on('end', function() {
		uc.end();
	});

	// uc.end();
}).catch(ugrid.onError);
