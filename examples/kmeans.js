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

	var N = 100000, D = 16, K = 4, ITERATIONS = 100;
	var points = ugrid.loadTestData(N, D).persist();
	var means = yield points.takeSample(K);
	for (var i = 0; i < K; i++) 
		means[i] = means[i].features;

	// Ensure unicity of initial K-means
	function compareVector(v1, v2) {
		for (var i = 0; i < v1.length; i++)
			if (v1[i] != v2[i])
				return false;
		return true;
	}

	console.log('\nInitial K-means');	
	// console.log(means);

	for (var i = 0; i < K - 1; i++)
		for (var j = i + 1; j < K; j++)
			if (compareVector(means[i], means[j]))
				throw 'Initial means must be distinct'	

	// console.log('D = ' + means[0].length);
	// var data = yield points.collect();
	// console.log('\nData :');
	// console.log(data);

	// BUG si on collect juste après le reduceByKey car le stage 1 est vide

	for (var i = 0; i < ITERATIONS; i++) {
		var startTime = new Date();
		
		var newMeans = yield points.map(ml.closestSpectralNorm, [means])
			.reduceByKey('cluster', ml.accumulate, {acc: ml.zeros(D), sum: 0})
			.map(function(a) {
				var res = [];
				for (var i = 0; i < a.acc.length; i++)
					res.push(a.acc[i] / a.sum);
				return res;
			}, [])
			.collect();
		var dist = 0;
		for (var k = 0; k < K; k++)
			for (var j = 0; j < means[k].length; j++)
				dist += Math.pow(newMeans[k][j] - means[k][j], 2);
		means = newMeans;
		var time = (new Date() - startTime) / 1000;
		console.log('\nIteration : ' + i + ', Time : ' + time);
		// Compare current K-means with previous iteration ones
		// console.log(newMeans)
		console.log('squared distance : ' + dist);		
		if (dist < 0.001)
			break;		
	}
	grid.disconnect();
})();
