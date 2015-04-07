#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../..');

if (process.argv.length != 5)
	throw 'Usage logreg-file.js file nIterations nMaxWorker'

var file = process.argv[2];
var nIterations = process.argv[3];
process.env.UGRID_WMAX = process.argv[4];

co(function *() {
	var uc = yield ugrid.context();

	var points = uc.textFile(file).map(function (e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), tmp];
	}).persist();

	var N = yield points.count();	// a recuperer dans la librairie ml
	var D = 16;						// a recuperer dans le dataset

	var model = new ugrid.ml.LogisticRegression(points, D, N);

	yield model.train(nIterations);

	console.log(model.w);

	uc.end();
}).catch(ugrid.onError);
