#!/usr/local/bin/node --harmony

var co = require('co');

var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();

	var N = 203472 * 4;					// Number of observations
	var D = 16;							// Number of features
	var seed = 1;
	var nIterations = 1;				// Number of iterations

	var points = uc.randomSVMData(N, D, seed).persist();
	// var points = uc.randomSVMData(N, D, seed);
	var model = new ugrid.ml.LogisticRegression(points, D, N);

	yield model.train(nIterations);

	console.log(model.w);

	// uc.end();
}).catch(ugrid.onError);
