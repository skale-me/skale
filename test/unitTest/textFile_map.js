#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');

// Create test file
var file = '/tmp/data.txt';
var a = '-1 1 1 1 1 1 1 1 1 1 1\n' +
	'-1 2 2 2 2 2 2 2 2 2 2\n' +
	'-1 3 3 3 3 3 3 3 3 3 3\n' +
	'-1 4 4 4 4 4 4 4 4 4 4';
fs.writeFileSync(file, a);

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function parse(e) {
		var tmp = e.split(' ').map(parseFloat);
		return {label: tmp.shift(), features: tmp};
	}

	var points = uc.textFile(file).map(parse).persist();
	var t0 = yield points.collect();
	console.log(t0);

	fs.unlink(file, function () {
		uc.end();
	});
}).catch(ugrid.onError);
