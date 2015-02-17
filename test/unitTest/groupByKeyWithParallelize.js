#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	var a = [[0, 1], [0, 2], [1, 3], [1, 4]];

	var points = yield ugrid.parallelize(a)
		.groupByKey()
		.collect();

	console.log(points);

	ugrid.end();
})();
