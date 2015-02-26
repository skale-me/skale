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

	var points = yield ugrid.textFile(file)
		.map(parse)
		.groupByKey()
		.collect();

	console.log(points)

	assert(points[0][1][0].features[0] == 1);
	assert(points[0][1][0].features[1] == 1);
	assert(points[0][1][1].features[0] == 3);
	assert(points[0][1][1].features[1] == 3);
	assert(points[1][1][0].features[0] == 2);
	assert(points[1][1][0].features[1] == 2);
	assert(points[2][1][0].features[0] == 4);
	assert(points[2][1][0].features[1] == 4);	

	fs.unlink(file, function (err) {
		ugrid.end();
	});
})();
