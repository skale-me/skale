// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

const thenify = require('thenify');

module.exports = StandardScaler;

function StandardScaler() {
  // Transform is defined here to be automatically serialized
  // along with object instance and be usable inside workers callbacks.
  this.transform = function (point) {
    let pointStd = [];
    for (let i = 0; i < point.length; i++)
      pointStd[i] = (point[i] - this.mean[i]) / this.std[i];
    return pointStd;
  };
  this.mean;
  this.std;
}

function meanReducer(acc, features) {
  for (let i = 0; i < features.length; i++)
    acc.sum[i] = (acc.sum[i] || 0) + features[i];
  acc.count++;
  return acc;
}

function meanCombiner(a, b) {
  if (a.sum.length === 0) return b;
  for (let i = 0; i < b.sum.length; i++)
    a.sum[i] += b.sum[i];
  a.count += b.count;
  return a;
}

function stddevReducer(acc, features) {
  for (let i = 0; i < features.length; i++)
    acc.sum[i] = (acc.sum[i] || 0) + (features[i] - acc.mean[i]) ** 2;
  return acc;
}

function stddevCombiner(a, b) {
  if (a.sum.length === 0) return b;
  for (let i = 0; i < b.sum.length; i++)
    a.sum[i] += b.sum[i];
  return a;
}

StandardScaler.prototype.fit = thenify(function (points, done) {
  const self = this;

  // Compute mean of each features
  points.aggregate(meanReducer, meanCombiner, {sum: [], count: 0}, function(err, data) {
    self.count = data.count;
    self.mean = [];
    for (let i = 0; i < data.sum.length; i++)
      self.mean[i] = data.sum[i] / data.count;

    // Now that we have the mean of each feature, let's compute their standard deviation
    points .aggregate(stddevReducer, stddevCombiner, {sum: [], mean: self.mean}, function(err, res) {
      self.std = [];
      for (let i = 0; i < res.sum.length; i++)
        self.std[i] = Math.sqrt(res.sum[i] / self.count);
      done();
    });
  });
});
