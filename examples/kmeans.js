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

	// var N = 100000;						// Number of observations	
	// var D = 16;						// Number of features
	// var K = 4;						// Number of clusters
	var N = 4000;						// Number of observations	
	var D = 2;						// Number of features
	var K = 2;						// Number of clusters
	var ITERATIONS = 10;				// Number of iterations
	var time = new Array(ITERATIONS);

	var points = ugrid.loadTestData(N, D).persist();
	var means = yield points.takeSample(K);
	for (i = 0; i < K; i++)
		means[i] = means[i].features;

	// Display input data
	// console.log('\nInitial K-means');
	// console.log(means);
	// var data = yield points.collect();
	// console.log('\nData :');
	// console.log(data);

	for (var i = 0; i < ITERATIONS; i++) {
		var startTime = new Date();
		var means = yield points.map(ml.closestSpectralNorm, [means])
			.reduceByKey('cluster', ml.accumulate, {acc: ml.zeros(D), sum: 0})
			.map(function(a) {
				var res = [];
				for (var i = 0; i < a.acc.length; i++)
					res.push(a.acc[i] / a.sum);
				return res;
			}, [])
			.collect();
		var endTime = new Date();
		time[i] = (endTime - startTime) / 1000;
		console.log('\nIteration : ' + i + ', Time : ' + time[i]);
		console.log(means);
		// Compare current K-means with previous iteration ones
	}
	console.log('\nFirst iteration : ' + time[0]);
	time.shift();
	console.log('Later iterations : ' + time.reduce(function(a, b) {return a + b}) / (ITERATIONS - 1));

	grid.disconnect();
})();
