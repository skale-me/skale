#!/usr/bin/env node

var skale = require('skale-engine');
var ml = require('skale-engine/ml');

var sc = skale.context();

var nObservations = 10;
var nFeatures = 16;
var nIterations = 10;
var seed = 1;

//var points = sc.randomSVMData(nObservations, nFeatures, seed).persist();
var points = sc.source(nObservations, ml.randomSVMLine, nFeatures).persist();

var model = new ml.LogisticRegressionWithSGD(points);

model.train(nIterations, function() {
  console.log(model.weights);
  sc.end();
});
