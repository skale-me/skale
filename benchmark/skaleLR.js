#!/usr/bin/env node
'use strict';

const sc = require('skale').context();

function logisticLossGradient(p, weights) {
  const grad = [];
  const label = p[0];
  const features = p[1];
  let dotProd = 0;

  for (let i = 0; i < features.length; i++)
    dotProd += features[i] * weights[i];

  const tmp = 1 / (1 + Math.exp(-dotProd)) - label;

  for (let i = 0; i < features.length; i++)
    grad[i] = features[i] * tmp;
  return grad;
}

function sum(a, b) {
  for (let i = 0; i < b.length; i++)
    a[i] += b[i];
  return a;
}

function featurize(line) {
  const tmp = line.split(' ').map(Number);
  const label = tmp.shift();  // [-1,1] labels
  const features = tmp;
  return [label, features];
}

const file = process.argv[2];
const nIterations = +process.argv[3] || 10;
const points = sc.textFile(file).map(featurize).persist();
const D = 16;
const stepSize = 1;
const regParam = 1;

const zero = Array(D).fill(0);
const weights = Array(D).fill(0);

if (!file) throw 'Usage: lr.js file [nIterations]';

points.count(function (err, data) {
  const N = data;
  let i = 0;

  function iterate() {
    points.map(logisticLossGradient, weights)
      .reduce(sum, zero)
      .then(function(gradient) {
        const iss = stepSize / Math.sqrt(i + 1);
        for (let j = 0; j < weights.length; j++) {
          weights[j] -= iss * (gradient[j] / N + regParam * weights[j]);
        }
        if (++i < nIterations) return iterate();
        console.log(weights);
        sc.end();
      });
  }
  iterate();
});
