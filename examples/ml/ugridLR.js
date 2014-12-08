#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var LogisticRegression = require('../../lib/classification/LogisticRegression.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

co(function *() {
	yield grid.connect();
	var res = yield grid.send('devices', {type: "worker"});
	var ugrid = new UgridContext(grid, res.devices);

	var N = 203472;						// Number of observations
	var D = 16;							// Number of features
	var P = 4;							// Number of partitions
	var nIterations = 20;				// Number of iterations

	var points = ugrid.loadTestData(N, D, P).persist();
	var model = yield LogisticRegression.train(points, D, nIterations);
	console.log(model);

	grid.disconnect();
})();
