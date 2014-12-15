#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var v = ml.randn(M);
//console.error(v);

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

try {
	co(function *() {
		var startTime = new Date();
		yield grid.connect();
		var devices = yield grid.send({cmd: 'devices', data: {type: "worker"}});
		var ugrid = new UgridContext(grid, devices);
		var a = ugrid.parallelize(v).persist();
		var r1 = yield a.collect();
		console.log(r1);
		var r2 = yield a.collect();
		console.error('distributed parallelize collect result')
		console.error(r1);
		console.error(r2);

		//compare r1 and r2
		test = true;
		for (var i = 0; i < r1.length; i++) {
			if (r1[i] != r2[i]) {
				test = false;
				break;
			}				
		}
		var endTime = new Date();
		if (test) {
			console.log('tesk ok');
			process.exit(0);
		} else {
			console.log('tesk ko');
			process.exit(1);
		}
		grid.disconnect();
	})();
} catch (err) {
	process.exit(2);
}
