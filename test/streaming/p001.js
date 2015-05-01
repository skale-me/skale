#!/usr/local/bin/node --harmony

// stream -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

var s1 = fs.createReadStream('./f', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var dist = [];

	var out = uc.stream(s1, {N: 2}).collect({stream: true});

	out.on('data', function(res) {
		dist.push(res);
	});

	out.on('end', function(res) {
		console.log(dist);
		console.assert(dist.length == 2);
		for (var i in dist)
			console.assert(dist[i].length == 2);
		uc.end();
	});
}).catch(ugrid.onError);
