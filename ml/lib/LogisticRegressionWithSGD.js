var thenify = require('thenify');

// Zeros function must be included in a separate utilities module
function zeros(N) {
	var w = new Array(N);
	for (var i = 0; i < N; i++)
		w[i] = 0;
	return w;
}

module.exports = function (points, options) {
	var self = this, options = options || {};
	this.points = points;
	this.weights = options.weights;					// May be undefined on startup
	this.stepSize = options.stepSize || 1;
	this.regParam = options.regParam || 1;
	this.D;
	this.N;

	// For now prediction returns a soft output, TODO: include threshold and hard output 
	this.predict = function(point) {
		var margin = 0;
		for (var i = 0; i < point.length; i++)
			margin += this.weights[i] * point[i];
		var prediction = 1 / (1 + Math.exp(-margin));
		return prediction;
	}

	this.train = thenify(function(nIterations, callback) {
		var i = 0;

		// If not yet know, we have to find out the number of features and the number of entries in training set
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

			self.points.aggregate(reducer, combiner, accumulator).then(function(result) {
				self.N = result.count;
				self.D = result.first[1].length;
				if (self.weights == undefined) self.weights = zeros(self.D);
				iterate();
			})
		} else iterate();

		function logisticLossGradient(p, args) {	// valid for labels in [-1, 1]
			var label = p[0], features = p[1], grad = [], dot_prod = 0, tmp, i;

			for (i = 0; i < features.length; i++)
				dot_prod += features[i] * args.weights[i];

			tmp = 1 / (1 + Math.exp(-dot_prod)) - label;

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
			self.points.map(logisticLossGradient, {weights: self.weights}).reduce(sum, zeros(self.D)).then(function(gradient) {
				var thisIterStepSize = self.stepSize / Math.sqrt(i + 1);
				for (var j = 0; j < self.weights.length; j++)
					self.weights[j] -= thisIterStepSize * (gradient[j] / self.N + self.regParam * self.weights[j]);	// L2 regularizer
				// for (var j = 0; j < self.weights.length; j++) {
				// 	self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1));									// zero regularizer	
				// 	// self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1)) + (self.weights[j] > 0 ? 1 : -1);	// L1 regularizer
				// 	// self.weights[j] -= gradient[j] / (self.N * Math.sqrt(i + 1)) + self.weights[j];					// L2 regularizer
				// }
				if (++i == nIterations) callback(null);
				else iterate();
			});
		}
	});
};