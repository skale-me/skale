// Unsupervised clusterization model using K-means
// Authors: M. Vertes (current), C. Artigue (preliminary)
// License: Apache License 2.0

'use strict';

const thenify = require('thenify');

function KMeans(nClusters, options) {
  if (!(this instanceof KMeans))
    return new KMeans(nClusters, options);
  options = options || {};
  this.nClusters = nClusters;
  this.maxMse = options.maxMse || 0.0000001;
  this.means = options.means;
  this.maxIterations = options.maxIterations || 100;

  // Return cluster index for which element is closest to center (euclidean norm)
  // Function is inlined in object (vs prototype) serialized to workers
  this.predict = function (element) {
    let means = this.means;
    let smallestSn = Infinity;
    let smallestSnIdx;
    for (let i = 0; i < means.length; i++) {
      let sn = 0;
      for (let j = 0; j < element.length; j++) {
        let delta = element[j] - means[i][j];
        sn += delta * delta;
      }
      if (sn < smallestSn) {
        smallestSnIdx = i;
        smallestSn = sn;
      }
    }
    return smallestSnIdx;
  };
}

KMeans.prototype.fit = thenify(function(trainingSet, done) {
  const self = this;
  let iter = 0;

  if (self.means === undefined) {
    trainingSet.takeSample(false, self.nClusters, function (err, means) {
      self.means = means;
      iterate();
    });
  } else iterate();

  function accumulate(a, b) {
    a.sum += b.sum;
    for (let i = 0; i < b.data.length; i++)
      a.data[i] += b.data[i];
    return a;
  }

  function iterate() {
    trainingSet
      .map((a, self) => [self.predict(a), {data: a, sum: 1}], self)
      .reduceByKey(accumulate, {data: Array(self.means.length).fill(0), sum: 0})
      .map(a => a[1].data.map(e => e / a[1].sum))
      .collect(function (err, means) {
        let mse = 0;
        for (let i = 0; i < self.nClusters; i++) {
          for (let j = 0; j < means[i].length; j++) {
            let delta = means[i][j] - self.means[i][j];
            mse += delta * delta;
          }
        }
        self.means = means;
        if (mse < self.maxMse || iter++ > self.maxIterations)
          return done();
        iterate();
      });
  }
});

module.exports = KMeans;
