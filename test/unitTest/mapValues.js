#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

var a = [[0, 1], [1, 2], [1, 3]];

co(function *() {
	yield ugrid.init();

	function by2 (e) {
		return e * 2;
	}

	var p1 = ugrid.parallelize(a).mapValues(by2);

	console.log(yield p1.collect());	

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
