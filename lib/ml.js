// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var thenify = require('thenify');
var Source = require('skale-engine').Source;

var ml = {};
module.exports = ml;

ml.randomSVMData = function (sc, N, D, seed, nPartitions) {
	function randomSVMLine(i, a, task) {
		var seed = i * (a.D + 1);
		var data = task.lib.ml.randn2(a.D + 1, seed);
		data[0] = Math.round(Math.abs(data[0])) * 2 - 1;
		return [data.shift(), data];
	}
	return new Source(sc, N, randomSVMLine, {D, seed}, nPartitions);
};

ml.randomSVMLine = function(rng, D) {
	var data = rng.randn(D + 1);
	data[0] = Math.round(Math.abs(data[0])) * 2 - 1;
	return [data.shift(), data];
};

/*
	Linear Models:
		- classification (logistic regression, SVM)
		- regression (least square, Lasso, ridge)
	NB:
		All those models can be trained using a stochastic gradient descent
		using different loss functions (logistic, hinge and squared) and different regularizers (Zero, L1, L2, elastic net)
*/

ml.StandardScaler = function() {
	var self = this;

	this.fit = function(points, done) {

		function reducer(acc, features) {
			for (var i in features) {
				if (acc.sum[i] == undefined) acc.sum[i] = 0;	// suboptimal initialization (number of features unknown ?)
				acc.sum[i] += features[i];
			}
			acc.count++;
			return acc;
		}

		function combiner(a, b) {
			if (a.sum.length == 0) return b;
			for (var i in b.sum) a.sum[i] += b.sum[i]
			a.count += b.count;
			return a;
		}

		// compute mean of each features
		points.aggregate(reducer, combiner, {sum: [], count: 0}).on('data', function(data) {
			self.count = data.count;		// store length of dataset
			self.mean = [];					// store mean value of each feature
			for (var i in data.sum)
				self.mean[i] = data.sum[i] / data.count;

			// Now that we have the mean of each features, let's compute their standard deviation
			function reducer(acc, features) {
				for (var i = 0; i < features.length; i++) {
					if (acc.sum[i] == undefined) acc.sum[i] = 0;	// suboptimal initialization (number of features unknown ?)
					var delta = features[i] - acc.mean[i];
					acc.sum[i] += delta * delta;
				}
				return acc;				
			}

			function combiner(a, b) {
				if (a.sum.length == 0) return b;
				for (var i = 0; i < b.sum.length; i++) 
					a.sum[i] += b.sum[i];
				return a;
			}

			points.aggregate(reducer, combiner, {sum: [], mean: self.mean}).on('data', function(res) {
				self.std = [];
				for (var i = 0; i < res.sum.length; i++)
					self.std[i] = Math.sqrt(res.sum[i] / self.count);
				done();
			});
		});
	}
}

ml.BinaryClassificationMetrics = function(predictionAndLabels) {
	var self = this;
	this.predictionAndLabels = predictionAndLabels;
	this.confusionMatrices;			// we'll compute it lazely !!
	this.threshold = [];
	var step = 1 / 10;
	for (var i = 0; i < 10; i++)
		this.threshold.push(i * step)

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

		self.predictionAndLabels.aggregate(reducer, combiner, accumulator).on('data', function(confusionMatrices) {
			self.confusionMatrices = confusionMatrices;
			done();
		});
	}

	this.precisionByThreshold = function(done) {
		if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computePrecisionByThreshold);
		else computePrecisionByThreshold();

		function computePrecisionByThreshold() {
			var precision = [];
			for (var i = 0; i < self.threshold.length; i++)
				precision[i] = [self.threshold[i], self.confusionMatrices.tp[i] / (self.confusionMatrices.tp[i] + self.confusionMatrices.fp[i])];
			done(precision);
		}
	}

	this.recallByThreshold = function(done) {
		if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeRecallByThreshold);
		else computeRecallByThreshold();

		function computeRecallByThreshold() {
			var recall = [];
			for (var i = 0; i < self.threshold.length; i++)
				recall[i] = [self.threshold[i], self.confusionMatrices.tp[i] / (self.confusionMatrices.tp[i] + self.confusionMatrices.fn[i])];
			done(recall);
		}
	}

	this.fMeasureByThreshold = function(done) {
		if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeFMeasureByThreshold);
		else computeFMeasureByThreshold();

		function computeFMeasureByThreshold() {
			var fmeasure = [];
			for (var i = 0; i < self.threshold.length; i++)
				fmeasure[i] = [self.threshold[i], 2 * self.confusionMatrices.tp[i] / (2 * self.confusionMatrices.tp[i] + self.confusionMatrices.fn[i] + self.confusionMatrices.fp[i])];
			done(fmeasure);
		}
	}

	this.accuracyByThreshold = function(done) {
		if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeAccuracyByThreshold);
		else computeAccuracyByThreshold();

		function computeAccuracyByThreshold() {
			var accuracy = [];
			for (var i = 0; i < self.threshold.length; i++)
				accuracy[i] = [self.threshold[i], (self.confusionMatrices.tp[i] + self.confusionMatrices.tn[i]) / self.confusionMatrices.n];
			done(accuracy);
		}
	}

	this.roc = function(done) {
		if (self.confusionMatrices == undefined) self.confusionMatrixByThreshold(computeROC);
		else computeROC();

		function computeROC() {
			var roc = [];
			for (var i = 0; i < self.threshold.length; i++) {
				var fpr = self.confusionMatrices.fp[i] / (self.confusionMatrices.fp[i] + self.confusionMatrices.tn[i]);
				var tpr = self.confusionMatrices.tp[i] / (self.confusionMatrices.tp[i] + self.confusionMatrices.fn[i]);
				roc[i] = [self.threshold[i], [fpr, tpr]];
			}
			done(roc);
		}
	}	
}

ml.LogisticRegression = function (points, options) {
	var self = this, options = options || {};
	this.points = points;
	this.weights = options.weights;				// May be undefined on startup
	this.stepSize = options.stepSize || 1;
	this.regParam = options.regParam || 1;
	this.D;
	this.N;

	this.train = thenify(function(nIterations, callback) {
		var i = 0;

		if ((self.D == undefined) || (self.N == undefined)) {
			var accumulator = {count: 0};

			function reducer(acc, b) {
				if (acc.first == undefined) acc.first = b;
				acc.count++;
				return acc;
			}

			function combiner(acc1, acc2) {
				if ((acc1.first == undefined) && (acc2.first)) acc1.first = acc2.first;
				acc1.count += acc2.count;
				return acc1;
			}

			self.points.aggregate(reducer, combiner, accumulator)
				.on('data', function(result) {
					self.N = result.count;
					self.D = result.first[1].length;
					if (self.weights == undefined) self.weights = zeros(self.D);
					iterate();
				})
		} else iterate();

		// OLD
		// function logisticLossGradient(p, args) {	// implementation for -1 and +1 labels
		// 	var grad = [], dot_prod = 0;
		// 	var label = -p[0];
		// 	var features = p[1];
		// 	for (var i = 0; i < features.length; i++)
		// 		dot_prod += features[i] * args.weights[i];

		// 	var t2 = Math.exp(label * dot_prod);
		// 	var tmp = t2 / (1 + t2) * label;
		// 	var loss = Math.log(1 + t2);

		// 	for (i = 0; i < features.length; i++)
		// 		grad[i] = features[i] * tmp;
		// 	return grad;
		// }

		// NEW
		function logisticLossGradient(p, args) {	// implementation for -1 and +1 labels
			var grad = [], dot_prod = 0;
			var label = p[0];
			var features = p[1];
			for (var i = 0; i < features.length; i++)
				dot_prod += features[i] * args.weights[i];

			var tmp = 1 / (1 + Math.exp(-dot_prod)) - label;

			for (var i = 0; i < features.length; i++)
				grad[i] = features[i] * tmp;
			return grad;
		}

		function sum(a, b) {
			for (var i = 0; i < b.length; i++)
				a[i] += b[i];
			return a;
		}

		function iterate() {
			self.points.map(logisticLossGradient, {weights: self.weights}).reduce(sum, zeros(self.D)).on('data', function(gradient) {
				var thisIterStepSize = self.stepSize / Math.sqrt(i + 1);
				for (var j = 0; j < self.weights.length; j++)
					self.weights[j] -= thisIterStepSize * (gradient[j] / self.N + self.regParam * self.weights[j]);	// L2 regularizer
				// for (var j = 0; j < self.weights.length; j++) {
				// 	self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1));									// zero regularizer	
				// 	// self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1)) + (self.weights[j] > 0 ? 1 : -1);	// L1 regularizer
				// 	// self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1)) + self.weights[j];					// L2 regularizer
				// }
				if (++i == nIterations) callback();
				else iterate();
			});
		}
	});
};

ml.LinearSVM = function (data, D, N, w) {
	var self = this;
	this.w = w || zeros(D);

	this.train = thenify(function(nIterations, callback) {
		var i = 0;
		iterate();

		function hingeLossGradient(p, args) {
			var grad = [], dot_prod = 0, label = p[0], features = p[1];
			for (var i = 0; i < features.length; i++)
				dot_prod += features[i] * args.weights[i];

			if (label * dot_prod < 1)
				for (var i = 0; i < features.length; i++) 
					grad[i] = -label * features[i];
			else
				for (var i = 0; i < features.length; i++) 
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
			data.map(hingeLossGradient, {weights: self.w}).reduce(sum, zeros(D), function(err, gradient) {
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
	this.w = w || zeros(D);

	this.train = thenify(function(nIterations, callback) {
		var i = 0;
		iterate();

		function squaredLossGradient(p, args) {
			var grad = [], dot_prod = 0, label = p[0], features = p[1];
			for (var i = 0; i < features.length; i++)
				dot_prod += features[i] * args.weights[i];
			for (var i = 0; i < features.length; i++) 
				grad[i] = (dot_prod - label) * features[i];
			return grad;
		}

		function sum(a, b) {
			for (var i = 0; i < b.length; i++)
				a[i] += b[i];
			return a;
		}

		function iterate() {
			console.time(i);
			data.map(squaredLossGradient, {weights: self.w}).reduce(sum, zeros(D)).on('data', function(gradient) {
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
// 	Load learning sets first, create decision tree root  node 'rootNode', add learning set S into root node as its subset.
// 	For rootNode, we compute Entropy(rootNode.subset) first
// 	If Entropy(rootNode.subset)==0, then 
// 		rootNode.subset consists of records all with the same value for the  categorical attribute, 
// 		return a leaf node with decision attribute:attribute value;
// 	If Entropy(rootNode.subset)!=0, then 
// 		compute information gain for each attribute left(have not been used in splitting), 
// 		find attribute A with Maximum(Gain(S,A)). 
// 		Create child nodes of this rootNode and add to rootNode in the decision tree. 
// 	For each child of the rootNode, apply 
// 		ID3(S,A,V) recursively until reach node that has entropy=0 or reach leaf node.
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
				.reduceByKey(accumulate, {data: zeros(D), sum: 0})
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

function zeros(N) {
	var w = new Array(N);
	for (var i = 0; i < N; i++)
		w[i] = 0;
	return w;
}

/*
	Random(initSeed)
		Simple seeded random number generator
	Methods:
		- Random.next(): Generates a number x, so as -1 < x < 1
		- Random.reset(): Reset seed to initial seed value
*/
function Random(initSeed) {
	this.seed = initSeed || 1;

	this.next = function () {
	    var x = Math.sin(this.seed++) * 10000;
	    return (x - Math.floor(x)) * 2 - 1;
	};

	this.reset = function () {
		this.seed = initSeed;
	};

	this.randn = function (N) {
		var w = new Array(N);
		for (var i = 0; i < N; i++)
			w[i] = this.next();
		return w;
	};

	this.nextDouble = function () {
		return 0.5 * this.next() + 0.5;			// Must be uniform, not gaussian
	};
}

function rand2(seed) {
	var x = Math.sin(seed) * 10000;
	return (x - Math.floor(x)) * 2 - 1;
}

function randn2(n, seed) {
	var a = new Array(n), i;
	for (i = 0; i < n; i++) a[i] = rand2(seed++);
	return a;
}

function Poisson(lambda, initSeed) {
	this.seed = initSeed || 1;

	var rng = new Random(initSeed);

	this.sample = function () {
		var L = Math.exp(-lambda), k = 0, p = 1;
		do {
			k++;
			p *= rng.nextDouble();
		} while (p > L);
		return k - 1;
	}
}

// Compute a checksum of an arbitrary object
function cksum(o) {
	var i, h = 0, s = o.toString(), len = s.length;
	for (i = 0; i < len; i++) {
		h = ((h << 5) - h) + s.charCodeAt(i);
		h = h & h;	// convert to 32 bit integer
	}
	return Math.abs(h);
}

ml.Random = Random;
ml.Poisson = Poisson;
ml.cksum = cksum;
ml.zeros = zeros;
ml.rand2 = rand2;
ml.randn2 = randn2;
