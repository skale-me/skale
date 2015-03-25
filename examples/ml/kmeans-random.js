#!/usr/local/bin/node --harmony

'use strict'

var co = require('co');

var ugrid = require('../../../ugrid/lib/ugrid-context.js')({data: {type: 'master'}});
var KMeans = require('../../../ugrid/lib/ugrid-ml.js').KMeans;

var N = 100;
var K = 2;
var ITERATIONS = 2;

co(function *() {
	yield ugrid.init();

	var data = [];
	var D = 2;

	for (var i = 0; i < N; i++) {
		var t0 = [];
		for (var j = 0; j < D; j++)
			t0.push(Math.random() * 2 - 1);
		data.push(t0);
	}

	var points = ugrid.parallelize(data).persist();
	var model = new KMeans(points, K);

	yield model.train(ITERATIONS);

	console.log(model.means)

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});


