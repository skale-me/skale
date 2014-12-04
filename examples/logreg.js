#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../lib/co-ugrid.js');
var UgridContext = require('../lib/ugrid-context.js');
var ml = require('../lib/ugrid-ml.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

co(function *() {
	yield grid.connect();
	var res = yield grid.send('devices', {type: "worker"});
	var ugrid = new UgridContext(grid, res.devices);

	var N = 203472;						// Number of observations
	var D = 16;							// Number of features
	var P = 4;							// Number of partitions
	var ITERATIONS = 100;				// Number of iterations
	var time = new Array(ITERATIONS);

	var w = ml.randn(D);
	var points = ugrid.loadTestData(N, D, P).persist();

	for (var i = 0; i < ITERATIONS; i++) {
		var startTime = new Date();

		var gradient = yield points.map(ml.logisticLossGradient, [w]).reduce(ml.sum, ml.zeros(D));
		for (var j = 0; j < w.length; j++)
			w[j] -= gradient[j];

		var endTime = new Date();
		time[i] = (endTime - startTime) / 1000;
		startTime = endTime;
		console.log('Iteration : ' + i + ', Time : ' + time[i]);
		// console.log(w);
	}
	console.log(w);
	console.log('First iteration : ' + time[0]);
	time.shift();
	console.log('Later iterations : ' + time.reduce(function(a, b) {return a + b}) / (ITERATIONS - 1));

	grid.disconnect();
})();
