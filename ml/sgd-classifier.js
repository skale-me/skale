// Stochastic Gradient Descent Classifier for Skale
// Authors: M. Vertes (current), C. Artigue (preliminary)
// License: Apache License 2.0

'use strict';

const thenify = require('thenify');

module.exports = SGDClassifier;

function SGDClassifier(options) {
  if (!(this instanceof SGDClassifier))
    return new SGDClassifier(options);
  options = options || {};
  this.weights = options.weights || [];
  this.stepSize = options.stepSize || 1;
  this.regParam = options.regParam || 1;
    
  if (!options.penalty)                this.regularize = regularizeL2;
  else if (options.penalty === 'l2')   this.regularize = regularizeL2;
  else if (options.penalty === 'l1')   this.regularize = regularizeL1;
  else if (options.penalty === 'none') this.regularize = regularizeNone;
  else throw 'Invalid penalty parameter: ' + options.penalty;

  if (!options.loss)                    this.loss = hingeLoss;
  else if (options.loss === 'hinge')    this.loss = hingeLoss;
  else if (options.loss === 'logistic') this.loss = logisticLoss;
  else if (options.loss === 'squared')  this.loss = squaredLoss;
  else throw 'Invalid loss parameter: ' + options.loss;

  // For now prediction returns a soft output, TODO: include threshold and hard output
  this.predict = function (point) {
    let margin = 0;
    for (let i = 0; i < point.length; i++)
      margin += (this.weights[i] || 0) * (point[i] || 0);
    return 1 / (1 + Math.exp(-margin));
  };
}

// A training iteration for a stochastic gradient descent classifier consists to:
//   - compute loss (price of inaccuracy) for each label/features of training set
//   - finalize gradient (sum and average loss per feature)
//   - regularize loss weigths using a penalty function of gradient

SGDClassifier.prototype.fit = thenify(function (trainingSet, nIterations, callback) {
  const self = this;
  let iter = 0;

  iterate();

  function iterate() {
    trainingSet
      .map(self.loss, self.weights)
      .aggregate(
        // Compute total loss per feature and number of samples
        (a, b) => {
          for (let i = 0; i < b.length; i++)
            a[0][i] = (a[0][i] || 0) + (b[i] || 0);
          a[1]++;
          return a;
        },
        (a, b) => {
          for (let i = 0; i < b[0].length; i++)
            a[0][i] = (a[0][i] || 0) + (b[0][i] || 0);
          a[1] += b[1];
          return a;
        },
        [[], 0],
        function (err, result) {
          const iterStepSize = self.stepSize / Math.sqrt(iter + 1);
          self.regularize(self.weights, result, iterStepSize, self.regParam);
          if (++iter === nIterations) callback();
          else iterate();
        }
      );
  }
});

function regularizeNone(weights, gradientCount) {
  const [gradient, count] = gradientCount;

  for (let i = 0; i < gradient.length; i++) {
    let grad = (gradient[i] || 0) / count;
    weights[i] = (weights[i] || 0) - grad;
  }
};

function regularizeL1(weights, gradientCount, stepSize) {
  const [gradient, count] = gradientCount;

  for (let i = 0; i < gradient.length; i++) {
    let grad = (gradient[i] || 0) / count;
    weights[i] = weights[i] || 0;
    weights[i] -= stepSize * grad + (weights[i] > 0 ? 1 : -1);
  }
};

function regularizeL2(weights, gradientCount, stepSize, regParam) {
  const [gradient, count] = gradientCount;

  for (let i = 0; i < gradient.length; i++) {
    let grad = (gradient[i] || 0) / count;
    weights[i] = weights[i] || 0;
    weights[i] -= stepSize * (grad + regParam * weights[i]);
  }
};

// TODO #1: elastic-net regularizer: combine L1 and L2 with an
// alpha parameter in range [0, 1] where 1 => L1, 0 => L2,
// in between: (alpha * L1) + ((1-alpha) * L2)
// May be merge L1 and L2 functions

// TODO #2: for each regularizer: set weight to 0 if regularization
// crosses 0 (sign change), to achieve feature selection (sparse models)

function hingeLoss(p, weights) {
  const [label, features] = p;
  const grad = [];
  let dotProd = 0;

  for (let i = 0; i < features.length; i++)
    dotProd += (features[i] || 0) * (weights[i] || 0);

  if (label * dotProd < 1)
    for (let i = 0; i < features.length; i++)
      grad[i] = -label * (features[i] || 0);
  else
    for (let i = 0; i < features.length; i++)
      grad[i] = 0;

  return grad;
}

// valid for labels in [-1, 1]
function logisticLoss(p, weights) {
  const [label, features] = p;
  const grad = [];
  let dotProd = 0;

  for (let i = 0; i < features.length; i++)
    dotProd += (features[i] || 0) * (weights[i] || 0);

  const tmp = 1 / (1 + Math.exp(-dotProd)) - label;

  for (let i = 0; i < features.length; i++)
    grad[i] = (features[i] || 0) * tmp;

  return grad;
}

function squaredLoss(p, weights) {
  const [label, features] = p;
  const grad = [];
  let dotProd = 0;

  for (let i = 0; i < features.length; i++)
    dotProd += (features[i] || 0) * (weights[i] || 0);

  for (let i = 0; i < features.length; i++)
    grad[i] = (dotProd - label) * (features[i] || 0);

  return grad;
}
