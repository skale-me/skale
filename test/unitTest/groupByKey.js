#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

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

	var points = yield ugrid.textFile(file)
		.map(parse)
		.groupByKey()
		.collect();
	console.log(points);
	// 	.map(parse)
	// 	.reduceByKey('cluster', reducer, {acc: [0, 0], sum: 0})
	// 	.map(function(a) {
	// 		var res = [];
	// 		for (var i = 0; i < a.acc.length; i++)
	// 			res.push(a.acc[i] / a.sum);
	// 		return res;
	// 	})
	// 	.collect();

	// assert(points[0][0] == 2);
	// assert(points[0][1] == 2);
	// assert(points[1][0] == 2);
	// assert(points[1][1] == 2);
	// assert(points[2][0] == 4);
	// assert(points[2][1] == 4);

	fs.unlink(file, function (err) {
		ugrid.end();
	});
})();
