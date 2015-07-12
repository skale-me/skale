'use strict';

var thenify = require('thenify');
var ml = {};
module.exports = ml;

ml.randomSVMLine = function(rng, D) {
	var data = rng.randn(D + 1);
	data[0] = Math.round(Math.abs(data[0])) * 2 - 1;
	return [data.shift(), data];
};

ml.LogisticRegression = function (data, D, N, w) {
	var self = this;
	this.w = w || zeros(D);

	this.train = thenify(function(nIterations, callback) {
		var i = 0;
		iterate();

		function logisticLossGradient(p, w) {
			var grad = [], dot_prod = 0;
			var label = -p[0];
			var features = p[1];
			for (var i = 0; i < features.length; i++)
				dot_prod += features[i] * w[i];

			var t2 = Math.exp(label * dot_prod);
			var tmp = t2 / (1 + t2) * label;
			var loss = Math.log(1 + t2);

			for (i = 0; i < features.length; i++)
				grad[i] = features[i] * tmp;
			return grad;
		}

		function sum(a, b) {
			for (var i = 0; i < b.length; i++)
				a[i] += b[i];
			return a;
		}

		function iterate() {
			console.time(i);
			data.map(logisticLossGradient, [self.w]).reduce(sum, zeros(D), function(err, gradient) {
				console.timeEnd(i);
				for (var j = 0; j < self.w.length; j++)
					self.w[j] -= gradient[j] / (N * Math.sqrt(i + 1));
				if (++i == nIterations) callback();
				else iterate();
			});
		}
	});
};

ml.KMeans = function (data, nClusters) {
	var seed = 1;
	var maxMse = 0.0000001;
	this.mse = [];
	var D;
	var self = this;

	this.closestSpectralNorm = function (element, means) {
		var smallestSn = Infinity;
		var smallestSnIdx = 0;
		for (var i = 0; i < means.length; i++) {
			var sn = 0;
			for (var j = 0; j < element.length; j++)
				sn += Math.pow(element[j] - means[i][j], 2);
			if (sn < smallestSn) {
				smallestSnIdx = i;
				smallestSn = sn;
			}
		}
		return [smallestSnIdx, {data: element, sum: 1}];
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
				a.acc[i] += b.data[i];
			return a;
		}

		function iterate() {
			console.time(i);
			data.map(self.closestSpectralNorm, [self.means])
				.reduceByKey(accumulate, {acc: zeros(D), sum: 0})
				.map(function(a) {
					return a[1].acc.map(function(e) {return e / a[1].sum;});
				}, [])
				.collect(function(err, newMeans) {
					console.timeEnd(i);
					var dist = 0;
					for (var k = 0; k < nClusters; k++)
						for (var j = 0; j < self.means[k].length; j++)
							dist += Math.pow(newMeans[k][j] - self.means[k][j], 2);
					self.means = newMeans;
					self.mse.push(dist);
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
ml.cksum = cksum;
ml.zeros = zeros;
