#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

// Create test file
var file = '/tmp/data.txt';
var a = '0 1 1\n' +
	'1 2 2\n' +
	'0 3 3\n' +
	'2 4 4';
fs.writeFileSync(file, a);

co(function *() {
	yield ugrid.init();

	function parse(e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), {features: tmp, sum: 1}];
	}

	function reducer(a, b) {
		a.sum += b.sum;
		for (var i = 0; i < b.features.length; i++)
			a.acc[i] += b.features[i];
		return a;
	}

	var points = yield ugrid.textFile(file)
		.map(parse)
		.reduceByKey(reducer, {acc: [0, 0], sum: 0})
		.map(function(a) {
			var res = [];
			for (var i = 0; i < a[1].acc.length; i++)
				res.push(a[1].acc[i] / a[1].sum);
			return res;
		})
		.collect();

	console.log(points)

	assert(points[0][0] == 2);
	assert(points[0][1] == 2);
	assert(points[1][0] == 2);
	assert(points[1][1] == 2);
	assert(points[2][0] == 4);
	assert(points[2][1] == 4);

	fs.unlink(file, function (err) {
		ugrid.end();
	});
})();
