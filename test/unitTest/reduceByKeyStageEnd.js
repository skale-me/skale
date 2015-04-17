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
		return [tmp.shift(), {array: tmp, sum: 1}];
	}

	function reducer(a, b) {
		a.sum += b.sum;
		for (var i = 0; i < b.array.length; i++)
			a.array[i] += b.array[i];
		return a;
	}

	var points = yield uc.textFile(file)
		.map(parse)
		.reduceByKey(reducer, {array: [0, 0], sum: 0})
		.collect();

	points.sort();

	console.assert(points[0][1].array[0] == 4);
	console.assert(points[0][1].array[1] == 4);
	console.assert(points[0][1].sum == 2);
	console.assert(points[1][1].array[0] == 2);
	console.assert(points[1][1].array[1] == 2);
	console.assert(points[1][1].sum == 1);
	console.assert(points[2][1].array[0] == 4);
	console.assert(points[2][1].array[1] == 4);
	console.assert(points[2][1].sum == 1);

	fs.unlink(file, function () {
		uc.end();
	});
}).catch(ugrid.onError);
