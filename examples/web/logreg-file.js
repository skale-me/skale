#!/usr/local/bin/node --harmony

var co = require('co');

if (process.argv.length != 4)
	throw 'Usage logreg-file.js file iterations'

var file = process.argv[2];
var iterations = process.argv[3];

var ugrid = require('../../../ugrid/lib/ugrid-context.js')();
var LogisticRegression = require('../../../ugrid/lib/ugrid-ml.js').LogisticRegression;

co(function *() {
	yield ugrid.init();

	var points = ugrid.textFile(file).map(function (e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), tmp];
	}).persist();

	var N = yield points.count();	// a recuperer dans la librairie ml
	var D = 16;						// a recuperer dans le dataset

	var model = new LogisticRegression(points, D, N);

	yield model.train(iterations);

	console.log(model.w);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
