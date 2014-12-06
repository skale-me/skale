#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../lib/ugrid-client.js');
var UgridContext = require('../lib/ugrid-context.js');
var ml = require('../lib/ugrid-ml.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

co(function *() {
	yield grid.connect();
	var res = yield grid.send('devices', {type: "worker"});
	var ugrid = new UgridContext(grid, res.devices);

	var N = 203472;						// Number of observations
	var D = 16;						// Number of features
	var K = 4;						// Number of clusters
	var ITERATIONS = 100;				// Number of iterations
	var time = new Array(ITERATIONS);

	var points = ugrid.loadTestData(N, D).persist();
	var means = yield points.takeSample(K);
	for (i = 0; i < K; i++)
		means[i] = means[i].features;
	// console.log('\nInitial K-means');
	// console.log(means);

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

	// Display input data
	// var data = yield points.collect();
	// console.log('\nData :');
	// console.log(data);

	for (i = 0; i < ITERATIONS; i++) {
		var startTime = new Date();
		var means = yield points.map(closestSpectralNorm, [means])
			.reduceByKey('cluster', accumulate, {acc: ml.zeros(D), sum: 0})
			.map(function(a) {
				var res = [];
				for (var i = 0; i < a.acc.length; i++)
					res.push(a.acc[i] / a.sum);
				return res;
			}, [])
			.collect();
		var endTime = new Date();
		time[i] = (endTime - startTime) / 1000;
		console.log('Iteration : ' + i + ', Time : ' + time[i]);
	}
	console.log('First iteration : ' + time[0]);
	time.shift();
	console.log('Later iterations : ' + time.reduce(function(a, b) {return a + b}) / (ITERATIONS - 1));

	grid.disconnect();
})();
