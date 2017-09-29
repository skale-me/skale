// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var thenify = require('thenify');

var ml = {};
module.exports = ml;

ml.StandardScaler = require('./standard-scaler.js');
ml.binaryClassificationMetrics = require('./binary-classification-metrics.js');
ml.LogisticRegression = require('./Logistic-regression.js');

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

ml.KMeans = function (data, nClusters, initMeans) {
  var seed = 1;
  var maxMse = 0.0000001;
  this.mse = [];
  this.means = initMeans;

  var D = initMeans ? initMeans[0].length : undefined ;
  var self = this;

  this.closestSpectralNorm = function (element, args) {
    var smallestSn = Infinity;
    var smallestSnIdx = 0;
    for (var i = 0; i < args.means.length; i++) {
      var sn = 0;
      for (var j = 0; j < element.length; j++)
        sn += Math.pow(element[1][j] - args.means[i][j], 2);
      if (sn < smallestSn) {
        smallestSnIdx = i;
        smallestSn = sn;
      }
    }
    return [smallestSnIdx, {data: element[1], sum: 1}];
  };

  this.train = thenify(function(nIterations, callback) {
    var i = 0;

    if (self.means === undefined) {
      console.time(i);
      data.takeSample(false, nClusters, seed, function(err, res) {
        console.timeEnd(i++);
        self.means = res;
        D = self.means[0].length;
        iterate();
      });
    } else iterate();

    function accumulate(a, b) {
      a.sum += b.sum;
      for (var i = 0; i < b.data.length; i++)
        a.data[i] += b.data[i];
      return a;
    }

    function iterate() {
      console.time(i);
      var newMeans = [];
      var res = data.map(self.closestSpectralNorm, {means: self.means})
        .reduceByKey(accumulate, {data: new Array(D).fill(0), sum: 0})
        .map(function(a) {
          return a[1].data.map(function(e) {return e / a[1].sum;});
        }, [])
        .collect();
      res.on('data', function(data) {
        newMeans.push(data);
      });
      res.on('end',function(){
        console.timeEnd(i);
        var dist = 0;
        for (var k = 0; k < nClusters; k++)
          for (var j = 0; j < self.means[k].length; j++)
            dist += Math.pow(newMeans[k][j] - self.means[k][j], 2);
        self.means = newMeans;
        self.mse.push(dist);
        console.log('mse: ' + dist);
        if ((dist < maxMse) || (++i == nIterations)) callback();
        else iterate();
      });
    }
  });
};
