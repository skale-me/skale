#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')();
var ml = require('../lib/ugrid-ml.js');

var file = process.argv[2];
var K = process.argv[3] || 2;
var ITERATIONS = process.argv[4] || 1;
var maxDist = 0.0001;

co(function *() {
	yield ugrid.init();

	var D = 16, seed = 1;

	function parse(e) {
		var t0 = e.split(' ').map(parseFloat);
		return {
			label: t0.shift(),
			features : t0
		}
	}

	var points = ugrid.textFile(file).map(parse).persist();
	var means = yield points.takeSample(K, seed);

	var i, j, k;
	for (i = 0; i < K; i++)
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

	for (i = 0; i < K - 1; i++)
		for (j = i + 1; j < K; j++)
			if (compareVector(means[i], means[j]))
				throw 'Initial means must be distinct';

	// console.log('D = ' + means[0].length);
	// var data = yield points.collect();
	// console.log('\nData :');
	// console.log(data);

	// BUG si on collect juste après le reduceByKey car le stage 1 est vide

	for (i = 0; i < ITERATIONS; i++) {
		// var startTime = new Date();

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
		for (k = 0; k < K; k++)
			for (j = 0; j < means[k].length; j++)
				dist += Math.pow(newMeans[k][j] - means[k][j], 2);
		means = newMeans;
		// var time = (new Date() - startTime) / 1000;
		// console.log('\nIteration : ' + i + ', Time : ' + time);
		// Compare current K-means with previous iteration ones
		// console.log(newMeans)
		// console.log('squared distance : ' + dist);
		if (dist < maxDist)
			break;
	}
	console.log(means);
	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
