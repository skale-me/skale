#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var ugrid = require('../..');
var ml = require('../../lib/ugrid-ml.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
	var frac = 0.4;
	var seed = 1;

	var points = yield uc.parallelize(v).sample(frac, seed).collect();

	console.log(points)

	uc.end();
}).catch(ugrid.onError);
