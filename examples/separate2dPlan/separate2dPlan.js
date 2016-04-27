#!/usr/bin/env node

var sc = require('skale-engine').context();
var LogisticRegression = require('../../lib/ml.js').LogisticRegression;
var StandardScaler = require('../../lib/ml.js').StandardScaler;

var training_set = sc.textFile('2D.data')
	.map(line => line.split(' ').map(str => str.trim()))
	.map(function(line) {return [Number(line[0]), [Number(line[1]), Number(line[2])]];})
	.persist();

// training_set.collect().on('data', console.log)

var scaler = new StandardScaler();
var features = training_set.map(point => point[1]);

// features.count().on('data', console.log)

scaler.fit(features, function done() {
	console.log('\nFinal scaler parameters')
	console.log(scaler)
});