var co = require('co');
var thunkify = require('thunkify');
var ml = require('../ugrid-ml.js');

function train_cb(points, D, nIterations, callback) {
	var rng = new ml.Random();
	var w = rng.randn(D);
	var time = new Array(nIterations);

	co(function *() {
		for (var i = 0; i < nIterations; i++) {
			var startTime = new Date();
			var gradient = yield points.map(ml.logisticLossGradient, [w]).reduce(ml.sum, ml.zeros(D));
			for (var j = 0; j < w.length; j++)
				w[j] -= gradient[j];

			var endTime = new Date();
			time[i] = (endTime - startTime) / 1000;
			startTime = endTime;
			console.log('\nIteration : ' + i + ', Time : ' + time[i]);
		}
		console.log('\nFirst iteration : ' + time[0]);
		time.shift();
		console.log('Later nIterations : ' + time.reduce(function(a, b) {return a + b}) / (nIterations - 1));
		callback(null, w);
	})();
}

module.exports.train_cb = train_cb;
module.exports.train = thunkify(train_cb);