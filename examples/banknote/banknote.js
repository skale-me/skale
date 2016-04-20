#!/usr/bin/env node

var ml = require('skale-ml');
// var ml = require('../../lib/ml.js');
var sc = require('skale-engine').context();

function parse(line) {
	var tmp = line.split(',').map(Number);
	var label = tmp.pop();
	var features = tmp;
	return [label, features]
}

var training_set = sc.textFile('data_banknote_authentication.txt').map(parse).persist();
var model = new ml.LogisticRegression(training_set);
var nIterations = 100;

model.train(nIterations, function() {
	var accumulator = {pos: 0, neg: 0, error: 0, n: 0, weights: model.weights};

	function reducer(acc, svm) {
		var tmp = 0;
		for (var i = 0; i < acc.weights.length; i++)
			tmp += acc.weights[i] * svm[1][i];
		var tmp2 = 1 / (1 + Math.exp(-tmp));
		var dec = tmp2 > 0.5 ? 1 : 0;
		if (dec == 0) acc.neg++; else acc.pos++;
		if (dec != svm[0]) acc.error++;
		acc.n++;
		return acc;
	}

	function combiner(acc1, acc2) {
		acc1.neg += acc2.neg;
		acc1.pos += acc2.pos;
		acc1.error += acc2.error;
		acc1.n += acc2.n;
		return acc1;
	}

	// Validate model manually
	training_set
		.aggregate(reducer, combiner, accumulator)
		.on('data', function(result) {
			console.log(result)
			console.log('Training error = ' + result.error / result.n * 100)
		})
		.on('end', sc.end)
});
