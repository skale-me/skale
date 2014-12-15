#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

co(function *() {
	yield grid.connect();
	var devices = yield grid.send({cmd: 'devices', data: {type: "worker"}});
	var ugrid = new UgridContext(grid, devices);

	var N = 4;
	var D = 2;
	var seed = 0;
	var res = yield ugrid.randomSVMData(N, D, seed).collect();		
	console.error(res);

	grid.disconnect();
})();
