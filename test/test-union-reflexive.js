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

	var N = 2;
	var D = 2;

	var d1 = ugrid.loadTestData(N, D).persist();
	var d2 = d1.union(d1);

	var r1 = yield d1.collect();
	console.log('d1: ');
	console.log(r1);

	var r2 = yield d2.collect();
	console.log('\nd2: ');
	console.log(r2);

	grid.disconnect();
})();

