#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var N = 10;					// Number of observations
	var D = 4;							// Number of features
	var P = 1;							// Number of partitions
	var seed = 1;
	var ITERATIONS = 5;				// Number of iterations
	var time = new Array(ITERATIONS);
	var rng = new ml.Random(seed);
	var rng2 = new ml.Random(seed);
	//~ var points = ugrid.randomSVMData(N, D, seed, P).persist(); // no randomSVMData in spark
	//create a vector V where each element is a json with 2 elements, "label" (1 / -1) and "features" an array of D elements
	var V = new Array(N);
	for (var i = 0; i < N; i++)
		V[i] = {
			label: 2 * Math.round(Math.abs(rng.randn(1))) - 1,
			features: rng.randn(D)
		};
	
	//~ console.log('DISPLAY V');
	//~ console.log(V);

	var w = rng2.randn(D);
	console.log(w);
	var points = ugrid.parallelize(V, P).persist();

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
	
	for (i = 0; i < w.length; i++)
		w[i] = Math.round(w[i]* 1e8) / 1e8 
	console.log(w);
	console.log('\nFirst iteration : ' + time[0]);
	time.shift();
	console.log('Later iterations : ' + time.reduce(function(a, b) {return a + b}) / (ITERATIONS - 1));

	ugrid.end();
})();
