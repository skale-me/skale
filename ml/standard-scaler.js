// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

var thenify = require('thenify');

module.exports = StandardScaler;

function StandardScaler() {
  // Transform is defined here to be automatically serialized
  // along with object instance and be usable inside workers callbacks.
  this.transform = function (point) {
    var pointStd = [];
    for (var i in point)
      pointStd[i] = (point[i] - this.mean[i]) / this.std[i];
    return pointStd;
  };
  this.mean;    // features means
  this.std;     // features std
}

function meanReducer(acc, features) {
  for (var i in features) {
    if (acc.sum[i] === undefined) acc.sum[i] = 0;  // suboptimal initialization (number of features unknown ?)
    acc.sum[i] += features[i];
  }
  acc.count++;
  return acc;
}

function meanCombiner(a, b) {
  if (a.sum.length === 0) return b;
  for (var i in b.sum) a.sum[i] += b.sum[i];
  a.count += b.count;
  return a;
}

function stddevReducer(acc, features) {
  for (var i = 0; i < features.length; i++) {
    if (acc.sum[i] === undefined) acc.sum[i] = 0;  // suboptimal initialization (as number of features is unknown)
    var delta = features[i] - acc.mean[i];
    acc.sum[i] += delta * delta;
  }
  return acc;
}

function stddevCombiner(a, b) {
  if (a.sum.length === 0) return b;
  for (var i = 0; i < b.sum.length; i++)
    a.sum[i] += b.sum[i];
  return a;
}

StandardScaler.prototype.fit = thenify(function (points, done) {
  var self = this;

  // Compute mean of each features
  points.aggregate(meanReducer, meanCombiner, {sum: [], count: 0}).then(function(data) {
    self.count = data.count;    // store length of dataset
    self.mean = [];         // store mean value of each feature
    for (var i in data.sum)
      self.mean[i] = data.sum[i] / data.count;

    // Now that we have the mean of each feature, let's compute their standard deviation
    points.aggregate(stddevReducer, stddevCombiner, {sum: [], mean: self.mean}).then(function(res) {
      self.std = [];
      for (var i = 0; i < res.sum.length; i++)
        self.std[i] = Math.sqrt(res.sum[i] / self.count);
      done();
    });
  });
});
