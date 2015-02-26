#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

// Create test file
var file = '/tmp/data.txt';
var a = '-1 1 1 1 1 1 1 1 1 1 1\n' +
	'-1 2 2 2 2 2 2 2 2 2 2\n' +
	'-1 3 3 3 3 3 3 3 3 3 3\n' +
	'-1 4 4 4 4 4 4 4 4 4 4';
fs.writeFileSync(file, a);

co(function *() {
	yield ugrid.init();

	function parse(e) {
		var tmp = e.split(' ').map(parseFloat);
		return {label: tmp.shift(), features: tmp};
	}

	var points = ugrid.textFile(file).map(parse).persist();
	var t0 = yield points.collect();
	console.log(t0);

	fs.unlink(file, function (err) {
		ugrid.end();
	});
})();
