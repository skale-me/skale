#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var a = [[0, 1], [0, 2], [1, 3], [1, 4]];

	var points = yield uc.parallelize(a)
		.groupByKey()
		.collect();

	console.log(points);

	uc.end();
}).catch(ugrid.onError);
