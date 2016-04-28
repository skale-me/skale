#!/usr/bin/env node
'use strict';

var sc = require('skale-engine').context();

function LogisticRegression(points, options) {
	options = options || {};
	this.points = points;
	this.weights = options.weights;
	this.stepSize = options.stepSize || 1;
	this.regParam = options.regParam || 1;
}

LogisticRegression.prototype.train = function (nIterations, callback) {
	var i = 0;
	var self = this;

	if ((self.D == undefined) || (self.N == undefined)) {
		self.points.aggregate(reducer, combiner, {count: 0})
			.on('data', function(result) {
				self.N = result.count;
				self.D = result.first[1].length;
				if (self.weights == undefined) self.weights = zeros(self.D);
				iterate();
			});
	} else iterate();

	function iterate() {
		self.points.map(logisticLossGradient, {weights: self.weights})
			.reduce(sum, zeros(self.D))
			.on('data', function(gradient) {
				var thisIterStepSize = self.stepSize / Math.sqrt(i + 1);
				for (var j = 0; j < self.weights.length; j++) {
					// L2 regularizer
					self.weights[j] -= thisIterStepSize * (gradient[j] / self.N + self.regParam * self.weights[j]);
				}
				if (++i == nIterations) callback();
				else iterate();
			});
	}
};

function reducer(acc, b) {
	if (acc.first == undefined) acc.first = b;
	acc.count++;
	return acc;
}

function combiner(acc1, acc2) {
	if ((acc1.first == undefined) && (acc2.first)) acc1.first = acc2.first;
	acc1.count += acc2.count;
	return acc1;
}

function logisticLossGradient(p, args) {
	var grad = [], dot_prod = 0;
	var label = p[0];
	var features = p[1];
	for (var i = 0; i < features.length; i++)
		dot_prod += features[i] * args.weights[i];

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

function zeros(N) {
	var w = new Array(N);
	for (var i = 0; i < N; i++)
		w[i] = 0;
	return w;
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
var model = new LogisticRegression(points);

if (!file) throw 'Usage: lr.js file [nIterations]';

model.train(nIterations, function() {
	var line = model.weights[0];
	for (var i = 1; i < model.weights.length; i++) 
		line += ' ' + model.weights[i];
	process.stdout.write(line + '\n');
	sc.end();
});
