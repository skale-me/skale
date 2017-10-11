// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

const thenify = require('thenify');

function reducer(acc, point) {
  let [prediction, label] = point;
  for (let i = 0; i < acc.length; i++) {
    let threshold = i / acc.length;
    if (prediction > threshold) {
      if (label > 0)
        acc[i].tp++;    // True positive
      else
        acc[i].fp++;    // False positive
    } else {
      if (label > 0)
        acc[i].fn++;    // False negative
      else
        acc[i].tn++;    // True negative
    }
  }
  return acc;
}

function combiner(acc1, acc2) {
  for (let i = 0; i < acc1.length; i++) {
    acc1[i].tp += acc2[i].tp;
    acc1[i].tn += acc2[i].tn;
    acc1[i].fp += acc2[i].fp;
    acc1[i].fn += acc2[i].fn;
  }
  return acc1;
}

// Compute area under curve, where curve is an Array of Objects {x, y}
function areaUnder(curve, sortx) {
  const sorted = sortx ? curve.sort((a, b) => a.x - b.x) : curve;
  let auc = 0;
  let {x, y} = sorted[0];

  for (let i = 0; i < sorted.length; i++) {
    let e = sorted[i];
    auc += (e.x - x) * (y + (e.y - y) / 2);
    x = e.x;
    y = e.y;
  }
  return auc;
}

const classificationMetrics = thenify(function (points, options, callback) {
  options = options || {};
  const steps = Number(options.steps) || 10;
  const init = Array(steps).fill({tp: 0, tn: 0, fp: 0, fn: 0}); // Confusion matrices

  points.aggregate(reducer, combiner, init, function (error, result) {
    result.map((e, i) => {
      e.threshold = i / steps;
      e.precision = e.tp / (e.tp + e.fp);           // Also called Positive Predictive Value (PPV)
      e.recall = e.tp / (e.tp + e.fn);              // Also called True Positive Rate (TPR) or sensitivity
      e.accuracy = (e.tp + e.tn) / (e.tp + e.tn + e.fp + e.fn);
      e.specificity = e.tn / (e.tn + e.fp);         // Also called True Negative Rate (TNR)
      e.fpr = e.fp / (e.fp + e.tn);
      e.f1 = 2 / (1 / e.recall + 1 / e.precision);  // F1 measure
      e.J = e.recall + e.specificity - 1;           // Younden's J statistic
      return e;
    });
    const auROC = areaUnder(result.map(a => ({x: a.fpr, y: a.recall})), true);
    const auPR = areaUnder(result.map(a => ({x: a.recall, y: a.precision})), true);
    const maxF1 = result.reduce((a, b) => a.f1 > b.f1 ? a : b, result[0]);
    callback(null, {rates: result, auROC: auROC, auPR: auPR, threshold: maxF1.threshold});
  });
});

module.exports = classificationMetrics;
