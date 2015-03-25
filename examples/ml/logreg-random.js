#!/usr/local/bin/node --harmony

var co = require('co');

var ugrid = require('../../../ugrid/lib/ugrid-context.js')({data: {type: 'master'}});
var LogisticRegression = require('../../../ugrid/lib/ugrid-ml.js').LogisticRegression;

co(function *() {
	yield ugrid.init();

	var N = 203472 * 4;					// Number of observations
	var D = 16;							// Number of features
	var seed = 1;
	var ITERATIONS = 10;				// Number of iterations

	var points = ugrid.randomSVMData(N, D, seed).persist();
	var model = new LogisticRegression(points, D, N);

	yield model.train(ITERATIONS);

	console.log(model.w);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
