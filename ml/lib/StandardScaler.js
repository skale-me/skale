var thenify = require('thenify');

module.exports = function() {
	var self = this;
	this.mean;			// features means
	this.std;			// features std 

	this.transform = function(point) {
		var point_std = [];
		for (var i in point)
			point_std[i] = (point[i] - this.mean[i]) / this.std[i];
		return point_std;
	}

	this.fit = thenify(function(points, done) {

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
		points.aggregate(reducer, combiner, {sum: [], count: 0}).then(function(data) {
			self.count = data.count;		// store length of dataset
			self.mean = [];					// store mean value of each feature
			for (var i in data.sum)
				self.mean[i] = data.sum[i] / data.count;

			// Now that we have the mean of each features, let's compute their standard deviation
			function reducer(acc, features) {
				for (var i = 0; i < features.length; i++) {
					if (acc.sum[i] == undefined) acc.sum[i] = 0;	// suboptimal initialization (as number of features is unknown)
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

			points.aggregate(reducer, combiner, {sum: [], mean: self.mean}).then(function(res) {
				self.std = [];
				for (var i = 0; i < res.sum.length; i++)
					self.std[i] = Math.sqrt(res.sum[i] / self.count);
				done();
			});
		});
	});
}
