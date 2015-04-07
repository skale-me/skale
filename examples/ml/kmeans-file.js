#!/usr/local/bin/node --harmony

'use strict'

var co = require('co');
var ugrid = require('../..');

var K = 2;
var iterations = 2;
var file = '/tmp/logreg.data';

co(function *() {
	var uc = yield ugrid.context();

	var points = uc.textFile(file).map(function (e) {
		return e.split(' ').map(parseFloat);
	}).persist();

	var model = new ugrid.ml.KMeans(points, K);

	yield model.train(iterations);

	console.log(model.means)

	uc.end();
}).catch(ugrid.onError);
