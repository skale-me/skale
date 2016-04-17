#!/usr/bin/env node

var skale = require('skale-engine');
var LogisticRegression = require('../../lib/ml.js').LogisticRegression; // var ml = require('skale-ml');
var randomSVMData = require('../../lib/ml.js').randomSVMData;

var nObservations = 10, nFeatures = 2, nIterations = 4, seed = 1, nPartitions = 1;

var sc = skale.context();

sc.on('connect', function() {console.log('Number of workers: %j', sc.worker.length);});

var points = randomSVMData(sc, nObservations, nFeatures, seed, nPartitions).persist();

// points.collect()
// 	.on('data', console.log)
// 	.on('end', sc.end)

var model = new LogisticRegression(sc, points, nFeatures, nObservations);

model.train(nIterations, function() {
	console.log(model.w);
	sc.end();
});
