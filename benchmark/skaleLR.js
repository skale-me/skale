#!/usr/bin/env node
'use strict';

var sc = require('skale-engine').context();

function logisticLossGradient(p, weights) {
	var grad = [], dot_prod = 0;
	var label = p[0];
	var features = p[1];
	for (var i = 0; i < features.length; i++)
		dot_prod += features[i] * weights[i];

	var tmp = 1 / (1 + Math.exp(-dot_prod)) - label;

	for (i = 0; i < features.length; i++)
		grad[i] = features[i] * tmp;
	return grad;
}

function sum(a, b) {
	for (var i = 0; i < b.length; i++)
		a[i] += b[i];
	return a;
}

function featurize(line) {
	var tmp = line.split(' ').map(Number);
	var label = tmp.shift();	// [-1,1] labels
	var features = tmp;
	return [label, features];
}

var file = process.argv[2];
var nIterations = process.argv[3] || 10;
var points = sc.textFile(file).map(featurize).persist();
var D = 16;
var stepSize = 1;
var regParam = 1;

var zero = Array(D).fill(0);
var weights = Array(D).fill(0);

if (!file) throw 'Usage: lr.js file [nIterations]';

points.count(function (err, data) {
	var N = data;
	var i = 0;

	function iterate() {
		points.map(logisticLossGradient, weights)
			.reduce(sum, zero)
			.then(function(gradient) {
				var iss = stepSize / Math.sqrt(i + 1);
				for (var j = 0; j < weights.length; j++) {
					weights[j] -= iss * (gradient[j] / N + regParam * weights[j]);
				}
				if (++i < nIterations) return iterate();
				console.log(weights);
				sc.end();
			});
	}
	iterate();
});
