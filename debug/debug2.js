#!/usr/bin/env node

var co = require('co');
var fs = require('fs');
var ugrid = require('../');

function textParser(line) {
	return line.split(' ').map(parseFloat);
}

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var s = fs.createReadStream('kv3', {enconding: 'utf8'});

	var d1 = uc.parallelize([1, 2, 3, 4, 5]);
	var d2 = uc.lineStream(s, {N : 2}).map(textParser);

	var dist = d1.union(d2).collect({stream: true});

	dist.on('data', function(d) {
		console.log(d);
	})

	dist.on('end', function() {
		uc.end();
	})


}).catch(ugrid.onError);
