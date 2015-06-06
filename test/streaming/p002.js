#!/usr/local/bin/node --harmony

// stream -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

var s1 = fs.createReadStream(__dirname + '/f', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var dist = [];

	var out = uc.lineStream(s1, {N: 3}).collect({stream: true});

	out.on('data', function(res) {
		dist.push(res);
	});

	out.on('end', function(res) {
		console.log(dist);
		console.assert(dist.length == 2);
		console.assert(dist[0].length == 3);
		console.assert(dist[1].length == 1);
		uc.end();
	});
}).catch(ugrid.onError);
