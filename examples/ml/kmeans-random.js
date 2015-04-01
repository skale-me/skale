#!/usr/local/bin/node --harmony

'use strict';

var co = require('co');
var ugrid = require('../..');

var N = 100;
var K = 2;
var ITERATIONS = 2;

co(function *() {
	var uc = yield ugrid.context();

	var data = [];
	var D = 2;

	for (var i = 0; i < N; i++) {
		var t0 = [];
		for (var j = 0; j < D; j++)
			t0.push(Math.random() * 2 - 1);
		data.push(t0);
	}

	var points = uc.parallelize(data).persist();
	var model = new ugrid.ml.KMeans(points, K);

	yield model.train(ITERATIONS);

	console.log(model.means)

	uc.end();
}).catch(ugrid.onError);
