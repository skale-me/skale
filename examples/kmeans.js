#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../lib/co-ugrid.js');
var UgridContext = require('../lib/ugrid-context.js');
var ml = require('../lib/ugrid-ml.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

co(function *() {
	yield grid.connect();
	var res = yield grid.send('devices', {type: "worker"});
	var ugrid = new UgridContext(grid, res.devices);

	var N = 4;						// Number of observations
	var D = 2;						// Number of features
	var K = 2;						// Number of clusters
	var ITERATIONS = 1;				// Number of iterations
	var time = new Array(ITERATIONS);

	var points = ugrid.loadTestData(N, D).persist();

	var means = yield points.takeSample(K);
	for (i = 0; i < K; i++)
		means[i] = means[i].features;
	console.log('\nInitial K-means');
	console.log(means);

	function closestSpectralNorm(element, means) {
		var smallestSn = Infinity;
		var smallestSnIdx = 0;
		for (var i = 0; i < means.length; i++) {
			var sn = 0;
			for (var j = 0; j < element.features.length; j++)
				sn += Math.pow(element.features[j] - means[i][j], 2);
			if (sn < smallestSn) {
				smallestSnIdx = i;
				smallestSn = sn;
			}
		}
		return {label: element.label, features: element.features, cluster: smallestSnIdx, sum: 1}
	}

	function accumulate(a, b) {
		a.sum += b.sum;
		for (var i = 0; i < b.features.length; i++)
			a.acc[i] += b.features[i];
		return a;
	}

	for (i = 0; i < ITERATIONS; i++) {
		// var startTime = new Date();

		// Detailed version
		var t0 = points.map(closestSpectralNorm, [means]);
		var t1 = yield t0.collect();
		console.log('\nClosest Spectral norm');
		console.log(t1);

		var initVal = {acc: ml.zeros(D), sum: 0};
		var t2 = points.map(closestSpectralNorm, [means]).reduceByKey('cluster', accumulate, initVal);
		var t3 = yield t2.collect();
		console.log('\nreduceByKey closest spectral norm: ');
		console.log(t3);

		// Compact version
		// var t0 = yield points.map(closestSpectralNorm, [means])
		// 	.reduceByKey('cluster', accumulate, {acc: ml.zeros(D), sum: 0})
		// 	.collect();
		// console.log(t0);

		// var endTime = new Date();
		// time[i] = (endTime - startTime) / 1000;
		// startTime = endTime;
		// console.log('Iteration : ' + i + ', Time : ' + time[i]);
	}
	// console.log('First iteration : ' + time[0]);
	// time.shift();
	// console.log('Later iterations : ' + time.reduce(function(a, b) {return a + b}) / (ITERATIONS - 1));

	grid.disconnect();
})();
