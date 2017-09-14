var thenify = require('thenify');

module.exports = function(predictionAndLabels) {
  var self = this;
  this.predictionAndLabels = predictionAndLabels;
  this.confusionMatrices;     // we'll compute it lazely !!
  this.threshold = [];
  var step = 1 / 10;        // 10 points threshold array
  for (var i = 0; i < 10; i++)
    this.threshold.push(i * step);

  this.confusionMatrixByThreshold = function(done) {
    // TODO: compute arrays from threshold array    
    var tp = [], tn = [], fp = [], fn = [];
    for (var i = 0; i < self.threshold.length; i++) {
      tp.push(0);
      tn.push(0);
      fp.push(0);           
      fn.push(0);
    }
    var accumulator = {tp: tp, tn: tn, fp: fp, fn: fn, n: 0, threshold: self.threshold};

    function reducer(acc, point) {
      var label = point[1], prediction = point[0];
      for (var i = 0; i < acc.threshold.length; i++) {
        var pred_label = prediction > acc.threshold[i] ? 1 : -1;
        if (pred_label == -1) {
          if (label == -1) acc.tn[i]++; else acc.fn[i]++;
        } else {
          if (label == -1) acc.fp[i]++; else acc.tp[i]++;
        }
      }
      acc.n++;
      return acc;
    }

    function combiner(acc1, acc2) {
      for (var i = 0; i < acc1.threshold.length; i++) {
        acc1.tp[i] += acc2.tp[i];
        acc1.tn[i] += acc2.tn[i];
        acc1.fp[i] += acc2.fp[i];
        acc1.fn[i] += acc2.fn[i];
      }
      acc1.n += acc2.n;
      return acc1;
    }

    self.predictionAndLabels.aggregate(reducer, combiner, accumulator).then(function(confusionMatrices) {
      self.confusionMatrices = confusionMatrices;
      done();
    });
  };

  this.precisionByThreshold = thenify(function(callback) {
    if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computePrecisionByThreshold);
    else computePrecisionByThreshold();

    function computePrecisionByThreshold() {
      var precision = [];
      for (var i = 0; i < self.threshold.length; i++)
        precision[i] = [self.threshold[i], self.confusionMatrices.tp[i] / (self.confusionMatrices.tp[i] + self.confusionMatrices.fp[i])];
      callback(null, precision);
    }
  });

  this.recallByThreshold = thenify(function(done) {
    if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeRecallByThreshold);
    else computeRecallByThreshold();

    function computeRecallByThreshold() {
      var recall = [];
      for (var i = 0; i < self.threshold.length; i++)
        recall[i] = [self.threshold[i], self.confusionMatrices.tp[i] / (self.confusionMatrices.tp[i] + self.confusionMatrices.fn[i])];
      done(null, recall);
    }
  });

  this.fMeasureByThreshold = thenify(function(beta, done) {
    var beta2 = beta * beta;
    if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeFMeasureByThreshold);
    else computeFMeasureByThreshold();

    function computeFMeasureByThreshold() {
      var fmeasure = [];
      for (var i = 0; i < self.threshold.length; i++)
        fmeasure[i] = [self.threshold[i], (1 + beta2) * self.confusionMatrices.tp[i] / ((1 + beta2) * self.confusionMatrices.tp[i] + beta2 * self.confusionMatrices.fn[i] + self.confusionMatrices.fp[i])];
      done(null, fmeasure);
    }
  });

  this.accuracyByThreshold = thenify(function(done) {
    if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeAccuracyByThreshold);
    else computeAccuracyByThreshold();

    function computeAccuracyByThreshold() {
      var accuracy = [];
      for (var i = 0; i < self.threshold.length; i++)
        accuracy[i] = [self.threshold[i], (self.confusionMatrices.tp[i] + self.confusionMatrices.tn[i]) / self.confusionMatrices.n];
      done(null, accuracy);
    }
  });

  this.roc = thenify(function(done) {
    if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeROC);
    else computeROC();

    function computeROC() {
      var roc = [];
      for (var i = 0; i < self.threshold.length; i++) {
        var fpr = self.confusionMatrices.fp[i] / (self.confusionMatrices.fp[i] + self.confusionMatrices.tn[i]);
        var tpr = self.confusionMatrices.tp[i] / (self.confusionMatrices.tp[i] + self.confusionMatrices.fn[i]);
        roc[i] = [self.threshold[i], [fpr, tpr]];
      }
      done(null, roc);
    }
  });
};
