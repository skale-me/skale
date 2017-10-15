// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var thenify = require('thenify');

var ml = {};
module.exports = ml;

ml.StandardScaler = require('./standard-scaler.js');
ml.classificationMetrics = require('./classification-metrics.js');
ml.SGDLinearModel = require('./sgd-linear-model.js');
ml.KMeans = require('./kmeans.js');

// Return a label -1 or 1, and features between -1 and 1
ml.randomSVMLine = function (index, D) {
  var label = Math.floor(Math.random() * 2) * 2 - 1;
  var features = [];
  for (var i = 0; i < D; i++)
    features.push(Math.random() * 2 - 1);
  return [label, features];
};

/*
  Linear Models:
    - classification (logistic regression, SVM)
    - regression (least square, Lasso, ridge)
  NB:
    All those models can be trained using a stochastic gradient descent
    using different loss functions (logistic, hinge and squared) and different regularizers (Zero, L1, L2, elastic net)
*/

ml.LinearSVM = function (data, D, N, w) {
  var self = this;
  this.w = w || new Array(D).fill(0);

  this.train = thenify(function(nIterations, callback) {
    var i = 0;
    iterate();

    function hingeLossGradient(p, args) {
      var grad = [], dotProd = 0, label = p[0], features = p[1];
      for (var i = 0; i < features.length; i++)
        dotProd += features[i] * args.weights[i];

      if (label * dotProd < 1)
        for (i = 0; i < features.length; i++) 
          grad[i] = -label * features[i];
      else
        for (i = 0; i < features.length; i++) 
          grad[i] = 0;

      return grad;
    }

    function sum(a, b) {
      for (var i = 0; i < b.length; i++)
        a[i] += b[i];
      return a;
    }

    function iterate() {
      console.time(i);
      data.map(hingeLossGradient, {weights: self.w}).reduce(sum, new Array(D).fill(0), function(err, gradient) {
        console.timeEnd(i);
        for (var j = 0; j < self.w.length; j++)
          self.w[j] -= gradient[j] / (N * Math.sqrt(i + 1));
        if (++i == nIterations) callback();
        else iterate();
      });
    }
  });
};

ml.LinearRegression = function (data, D, N, w) {
  var self = this;
  this.w = w || new Array(D).fill(0);

  this.train = thenify(function(nIterations, callback) {
    var i = 0;
    iterate();

    function squaredLossGradient(p, args) {
      var grad = [], dotProd = 0, label = p[0], features = p[1];
      for (var i = 0; i < features.length; i++)
        dotProd += features[i] * args.weights[i];
      for (i = 0; i < features.length; i++) 
        grad[i] = (dotProd - label) * features[i];
      return grad;
    }

    function sum(a, b) {
      for (var i = 0; i < b.length; i++)
        a[i] += b[i];
      return a;
    }

    function iterate() {
      console.time(i);
      data.map(squaredLossGradient, {weights: self.w}).reduce(sum, new Array(D).fill(0)).on('data', function(gradient) {
        console.timeEnd(i);
        for (var j = 0; j < self.w.length; j++)
          self.w[j] -= gradient[j] / (N * Math.sqrt(i + 1));
        if (++i == nIterations) callback();
        else iterate();
      });
    }
  });
};

// Decision tree basic unoptimized algorithm
// Begin ID3
//  Load learning sets first, create decision tree root  node 'rootNode', add learning set S into root node as its subset.
//  For rootNode, we compute Entropy(rootNode.subset) first
//  If Entropy(rootNode.subset)==0, then 
//    rootNode.subset consists of records all with the same value for the  categorical attribute, 
//    return a leaf node with decision attribute:attribute value;
//  If Entropy(rootNode.subset)!=0, then 
//    compute information gain for each attribute left(have not been used in splitting), 
//    find attribute A with Maximum(Gain(S,A)). 
//    Create child nodes of this rootNode and add to rootNode in the decision tree. 
//  For each child of the rootNode, apply 
//    ID3(S,A,V) recursively until reach node that has entropy=0 or reach leaf node.
// End ID3  

