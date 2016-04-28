#!/usr/bin/env node
'use strict';

var sc = require('skale-engine').context();
var ml = require('skale-ml');
// var ml = require('../../index.js');

// NB: our implementation is using [-1, 1] labels, spark uses [0, 1]
function featurize(line) {
	var tmp = line.split(' ').map(Number);
	var label = tmp.shift();	// [-1,1] labels
	var features = tmp;
	return [label, features];
}

var file = process.argv[2] || '1MB.dat';
var nIterations = process.argv[3] || 10;
var points = sc.textFile(file).map(featurize).persist();
var model = new ml.LogisticRegressionWithSGD(points);

model.train(nIterations, function() {
	var line = model.weights[0];
	for (var i = 1; i < model.weights.length; i++) 
		line += ' ' + model.weights[i]
	process.stdout.write(line + '\n');
	sc.end();
});
