#!/usr/bin/env node
'use strict';

var skale = require('skale-engine');
var ml = require('skale-ml');

var nObservations = Math.pow(2, 20);
var nFeatures = 16;
var nIterations = 10;
var seed = 1;

var sample = [1, []];
for (var i = 0; i < nFeatures; i++)
	sample[1].push(Math.random());
var approx_set_size = nObservations * JSON.stringify(sample).length;

console.log('\# Generating an in-memory random SVM dataset with');
console.log('\t - Number of observations: ' + nObservations);
console.log('\t - Number of features per observation: ' + nFeatures);
console.log('\t - Number of iterations: ' + nIterations);
console.log('\t - Approximate SVM file size on disk: ' + Math.ceil(approx_set_size / (1024 * 1024)) + ' Mb\n');
console.log('# Duration of each SGD iteration to compute binary logistic regression model:');

var sc = skale.context();
var points = ml.randomSVMData(sc, nObservations, nFeatures, seed).persist();
var model = new ml.LogisticRegression(points);

var i = 0;

function done() {
	console.timeEnd(i);
	if (++i < nIterations) {
		console.time(i);
		model.train(1, done);
	} else {
		console.log(model.weights)
		sc.end();
	}
}

console.time(i);
model.train(1, done);
