#!/usr/bin/env node
'use strict';

var skale = require('skale-engine');
var sizeOf = require('object-sizeof');
var ml = require('../lib/ml.js'); // var ml = require('skale-ml');

var nObservations = 10, nFeatures = 2, nIterations = 4, seed = 1, nPartitions = 3;

var sc = skale.context();

var points = ml.randomSVMData(sc, nObservations, nFeatures, seed, nPartitions).persist();

// points.collect()
// 	.on('data', console.log)
// 	.on('end', sc.end)

var model = new ml.LogisticRegression(sc, points, nFeatures, nObservations);

// sc.on('connect', function() {console.log('Number of workers: %j', sc.worker.length);});

model.train(nIterations, function() {
	console.log(model.w);
	sc.end();
});
