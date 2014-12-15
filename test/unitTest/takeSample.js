#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

co(function *() {
	yield grid.connect();
	var devices = yield grid.send({cmd: 'devices', data: {type: "worker"}});
	var ugrid = new UgridContext(grid, devices);

	var N = 10;
	var n = 3;
	var V = [];

	for (var i = 0; i < N; i++) 
		V[i] = i;

	var d1 = yield ugrid.parallelize(V).takeSample(n);

	console.error(V);
	console.error();
	console.error(d1);

	if (d1.length == n ) {
		
		for ( var i = 0; i < d1.length; i++) {
			if ( V.indexOf(d1[i]) == -1) {
				console.log("test ko");
				process.exit(1); //test KO
			} else {
				console.log("test ok");
				process.exit(0); //test OK
			} 
		}
	} 
	grid.disconnect();
})();

