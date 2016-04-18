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

console.log('\# Generating an in-memory random SVM dataset with')
console.log('\t - Number of observations: ' + nObservations);
console.log('\t - Number of features per observation: ' + nFeatures);
console.log('\t - Number of iterations: ' + nIterations);
console.log('\t - Approximate SVM file size on disk: ' + Math.ceil(approx_set_size / (1024 * 1024)) + ' Mb\n');
console.log('# Duration of each SGD iteration to compute binary logistic regrssion model:')

var sc = skale.context();
var set = ml.randomSVMData(sc, nObservations, nFeatures, seed).persist();
var model = new ml.LogisticRegression(sc, set, nFeatures, nObservations);

model.train(nIterations, function() {
	console.log(model.w);
	sc.end();
});
