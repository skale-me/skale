#!/usr/local/bin/node --harmony
var co = require('co');
var UgridClient = require('../lib/ugrid-client.js');
var UgridContext = require('../lib/ugrid-context.js');
var ml = require('../lib/ugrid-ml.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});
co(function *() {
	yield grid.connect();
	var devices = yield grid.send({cmd: 'devices', data: {type: 'worker'}});
	var ugrid = new UgridContext(grid, devices);

	// Bug on initial kmeans with 1 workers with this setup
	var N = 100, D = 4, K = 2, ITERATIONS = 100;
	var points = ugrid.loadTestData(N, D).persist();
	var means = yield points.takeSample(K);
	for (var i = 0; i < K; i++) 
		means[i] = means[i].features;
	// Verify unicity of initial means
	for (var i = 0; i < K; i++) {
		if (means.lastIndexOf(means[i]) != i)
			throw 'Initial means must be distinct'		
	}
	console.log('\nInitial K-means');	
	console.log(means);
	// console.log('D = ' + means[0].length);
	// var data = yield points.collect();
	// console.log('\nData :');
	// console.log(data);

	// BUG si on collect juste après le reduceByKey car le stage 1 est vide

	for (var i = 0; i < ITERATIONS; i++) {
		var newMeans = yield points.map(ml.closestSpectralNorm, [means])
			.reduceByKey('cluster', ml.accumulate, {acc: ml.zeros(D), sum: 0})
			.map(function(a) {
				var res = [];
				for (var i = 0; i < a.acc.length; i++)
					res.push(a.acc[i] / a.sum);
				return res;
			}, [])
			.collect();
		// Compare current K-means with previous iteration ones
		console.log('\nnewMeans')
		console.log(newMeans)
		var dist = 0;
		for (var k = 0; k < K; k++)
			for (var j = 0; j < means[k].length; j++)
				dist += Math.pow(newMeans[k][j] - means[k][j], 2);
		means = newMeans;
		console.log('\nIteration : ' + i);
		// console.log(newMeans);
		console.log('squared distance : ' + dist);		
		if (dist < 0.001)
			break;
	}
	grid.disconnect();
})();
