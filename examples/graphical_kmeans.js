#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')();
var ugrid2 = require('../lib/ugrid-client.js')({data: {type: 'ping'}});
var ml = require('../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();
	yield ugrid2.connect();

	var plotter = yield ugrid2.devices({type: 'plotter'});

	var N = 1000000, D = 16, K = 4, ITERATIONS = 100, seed = 1;
	var points = ugrid.randomSVMData(N, D).persist();
	var means = yield points.takeSample(K, seed);
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
		if (plotter[0]) {
			if (i > 0)
				ugrid2.send({cmd: 'plot', data: dist, id: plotter[0].id});
		}
		// if (dist < 0.00001) break;
	}
	ugrid.end();
	ugrid2.disconnect();
})();
