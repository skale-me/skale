#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');

// Create test file
var file = '/tmp/data.txt';
var a = '0 1 1\n' +
	'1 2 2\n' +
	'0 3 3\n' +
	'2 4 4';
fs.writeFileSync(file, a);

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function parse(e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), {features: tmp, sum: 1}];
	}

	var points = yield uc.textFile(file)
		.map(parse)
		.groupByKey()
		.collect();

	console.log(points)

	console.assert(points[0][1][0].features[0] == 1);
	console.assert(points[0][1][0].features[1] == 1);
	console.assert(points[0][1][1].features[0] == 3);
	console.assert(points[0][1][1].features[1] == 3);
	console.assert(points[1][1][0].features[0] == 2);
	console.assert(points[1][1][0].features[1] == 2);
	console.assert(points[2][1][0].features[0] == 4);
	console.assert(points[2][1][0].features[1] == 4);	

	fs.unlink(file, function (err) {
		uc.end();
	});
}).catch(ugrid.onError);
