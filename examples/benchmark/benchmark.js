#!/usr/bin/env node
'use strict';

var skale = require('skale-engine');
// var ml = require('skale-ml');
var ml = require('../../lib/ml.js');

// NB: our implementation is using [-1, 1] labels, spark uses [0, 1]

function featurize(line) {
	var tmp = line.split(' ').map(Number);
	// var label = (tmp.shift() + 1) / 2;	// transform label from [-1, 1] to [0, 1]
	var label = tmp.shift();	// [-1,1] labels
	var features = tmp;
	return [label, features];
}

var file = process.argv[2] || '1MB.dat';
var nIterations = process.argv[3] || 100;
var sc = skale.context();
var points = sc.textFile('1MB.dat').map(featurize).persist();
var model = new ml.LogisticRegression(points);

// points.count().on('data', console.log)

model.train(nIterations, function() {
	var line = model.weights[0];
	for (var i = 1; i < model.weights.length; i++) 
		line += ' ' + model.weights[i]
	process.stdout.write(line + '\n');
	sc.end();
});
