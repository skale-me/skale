// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

// Logistic regression with stochastic gradient descent

'use strict';

const thenify = require('thenify');

module.exports = LogisticRegression;

function LogisticRegression(dataset, options) {
  if (!(this instanceof LogisticRegression))
    return new LogisticRegression(dataset, options);
  options = options || {};
  this.weights = options.weights;         // May be undefined on startup
  this.stepSize = options.stepSize || 1;
  this.regParam = options.regParam || 1;
  this.D;
  this.N;
  // For now prediction returns a soft output, TODO: include threshold and hard output
  this.predict = function (point) {
    let margin = 0;
    for (let i = 0; i < point.length; i++)
      margin += this.weights[i] * point[i];
    let prediction = 1 / (1 + Math.exp(-margin));
    return prediction;
  };
}

LogisticRegression.prototype.train = thenify(function (trainingSet, nIterations, callback) {
  var self = this;
  var i = 0;

  // If undefined, find out the number of features and the number of entries in training set
  if (self.D === undefined || self.N === undefined) {
    trainingSet.aggregate(
      (acc, val) => ({first: acc.first || val, count: acc.count + 1}),
      (acc1, acc2) => ({first: acc1.first || acc2.first, count: acc1.count + acc2.count}),
      {count: 0}
    ).then(function(result) {
      self.N = result.count;
      self.D = result.first[1].length;
      if (self.weights === undefined) self.weights = new Array(self.D).fill(0);
      iterate();
    });
  } else iterate();

  function iterate() {
    trainingSet
      .map(logisticLossGradient, self.weights)
      .reduce((a, b) => a.map((e, i) => e + b[i]), new Array(self.D).fill(0))
      .then(function(gradient) {
        var thisIterStepSize = self.stepSize / Math.sqrt(i + 1);
        console.log('N:', self.N, 'i:', i);
        for (var j = 0; j < self.weights.length; j++) {
          console.log('j:', j, 'weigth:', self.weights[j]);
          console.log('j:', j, 'gradient:', gradient[j] || 0);
          self.weights[j] -= thisIterStepSize * (gradient[j] / self.N + self.regParam * self.weights[j]); // L2 regularizer
          //self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1)) + (self.weights[j] > 0 ? 1 : -1); // L1 regularizer
          console.log('j:', j, 'weigth:', self.weights[j]);
        }
        //  self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1));                 // zero regularizer
        // for (var j = 0; j < self.weights.length; j++) {
        //  self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1));                 // zero regularizer
        //  // self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1)) + (self.weights[j] > 0 ? 1 : -1); // L1 regularizer
        //  // self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1)) + self.weights[j];          // L2 regularizer
        // }
        if (++i === nIterations) callback(null);
        else iterate();
      });
  }
});

// valid for labels in [-1, 1]
function logisticLossGradient(p, weights) {
  var label = p[0], features = p[1], grad = [], dotProd = 0, tmp, i;

  for (i = 0; i < features.length; i++)
    dotProd += features[i] * weights[i];

  tmp = 1 / (1 + Math.exp(-dotProd)) - label;

  for (i = 0; i < features.length; i++)
    grad[i] = features[i] * tmp;

  return grad;
}
