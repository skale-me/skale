#!/usr/local/bin/node --harmony

'use strict'

var co = require('co');

var ugrid = require('../../../ugrid/lib/ugrid-context.js')({data: {type: 'master'}});
var KMeans = require('../../../ugrid/lib/ugrid-ml.js').KMeans;

var K = 2;
var iterations = 2;
var file = '/tmp/logreg.data';

co(function *() {
	yield ugrid.init();

	var points = ugrid.textFile(file).map(function (e) {
		return e.split(' ').map(parseFloat);
	}).persist();

	var model = new KMeans(points, K);

	yield model.train(iterations);

	console.log(model.means)

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});


