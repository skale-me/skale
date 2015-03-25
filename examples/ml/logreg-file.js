#!/usr/local/bin/node --harmony

var co = require('co');

var ugrid = require('../../../ugrid/lib/ugrid-context.js')({data: {type: 'master'}});
var LogisticRegression = require('../../../ugrid/lib/ugrid-ml.js').LogisticRegression;

co(function *() {
	yield ugrid.init();

	var ITERATIONS = 10;				// Number of iterations
	var file = '/tmp/logreg.data';		// a passer en argument

	var points = ugrid.textFile(file).map(function (e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), tmp];
	}).persist();

	var N = yield points.count();	// a recuperer dans la librairie ml
	var D = 16;						// a recuperer dans le dataset

	var model = new LogisticRegression(points, D, N);

	yield model.train(ITERATIONS);

	console.log(model.w);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
