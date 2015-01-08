#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')();
var ml = require('../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var N = 203472 * 4;					// Number of observations
	var D = 16;							// Number of features
	var P = 4;							// Number of partitions
	var seed = 1;
	var ITERATIONS = 20;				// Number of iterations
	var time = new Array(ITERATIONS);
	var rng = new ml.Random();
	var w = rng.randn(D);

	var points = ugrid.randomSVMData(N, D, seed, P).persist();

	for (var i = 0; i < ITERATIONS; i++) {
		var startTime = new Date();
		var gradient = yield points.map(ml.logisticLossGradient, [w]).reduce(ml.sum, ml.zeros(D));
		for (var j = 0; j < w.length; j++)
			w[j] -= gradient[j];
		var endTime = new Date();
		time[i] = (endTime - startTime) / 1000;
		startTime = endTime;
		console.log('\nIteration : ' + i + ', Time : ' + time[i]);
	}
	console.log(w);
	console.log('\nFirst iteration : ' + time[0]);
	time.shift();
	console.log('Later iterations : ' + time.reduce(function(a, b) {return a + b}) / (ITERATIONS - 1));

	ugrid.end();
})();
