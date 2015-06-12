#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../');

function textParser(line) {
	return line.split(' ').map(parseFloat);
}

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var s1 = fs.createReadStream('kv', {enconding: 'utf8'});
	var s2 = fs.createReadStream('kv2', {enconding: 'utf8'});

	var d1 = uc.lineStream(s1, {N : 1}).map(textParser);
	var d2 = uc.lineStream(s2, {N : 1}).map(textParser);

	var dist = d1.union(d2).collect({stream: true});

	dist.on('data', function(d) {
		console.log(d);
	})

	dist.on('end', function() {
		uc.end();
	})


}).catch(ugrid.onError);

