#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../..');
var LogisticRegression = ugrid.ml.LogisticRegression;

co(function *() {
	var uc = yield ugrid.context();

	var ITERATIONS = 10;
	var file = '/tmp/logreg.data';

	var points = uc.textFile(file).map(function (e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), tmp];
	}).persist();

	var N = yield points.count();
	var D = 10;						// à récupérer dans le dataset

	var model = new LogisticRegression(points, D, N);

	yield model.train(ITERATIONS);
	console.log(model.w);
	uc.end();
}).catch(ugrid.onError);
