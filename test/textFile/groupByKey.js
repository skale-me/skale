#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');
var groupByKey = require('../ugrid-test.js').groupByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, [1, 1]], [1, [2, 2]], [0, [3, 3]], [2, [4, 4]]];
	var loc = groupByKey(v);

	var file = '/tmp/data.txt';
	var str = v.reduce(function(a, b) {return a + ((b[0] + ' ' + b[1]).replace(',', ' ') + '\n')}, '');
	fs.writeFileSync(file, str.substr(0, str.length - 1));

	function parse(e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), tmp];
	}

	var dist = yield uc.textFile(file).map(parse).groupByKey().collect();

	loc.sort();
	dist.sort();
	for (var i = 0; i < dist.length; i++) {
		loc[i][1].sort();
		dist[i][1].sort();
		console.assert(JSON.stringify(loc[i]) == JSON.stringify(dist[i]));
	}

	fs.unlink(file, function (err) {uc.end();});
}).catch(ugrid.onError);
