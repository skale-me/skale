#!/usr/local/bin/node --harmony

// stream1 union stream2 -> collect

var fs = require('fs');
var co = require('co');
var ugrid = require('../..');

var s1 = fs.createReadStream(__dirname + '/f', {encoding: 'utf8'});
var s2 = fs.createReadStream(__dirname + '/f2', {encoding: 'utf8'});

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var dist = [], cnt = 0;

	var us1 = uc.lineStream(s1, {N: 4});
	var out = uc.lineStream(s2, {N: 4}).union(us1).collect({stream: true});

	out.on('data', function(res) {dist.push(res);});

	out.on('end', function() {
		console.log(dist);
		console.assert(dist.length == 1);
		console.assert(dist[0].length == 8);		
		uc.end();
	})
}).catch(ugrid.onError);
