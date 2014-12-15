#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var a = ml.randn(M);
console.error(a);

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});
var name = 'parallelize_collect';

try {
	co(function *() {
		var startTime = new Date();
		yield grid.connect();
		var devices = yield grid.send({cmd: 'devices', data: {type: "worker"}});
		var ugrid = new UgridContext(grid, devices);
		
		var res = yield ugrid.parallelize(a).collect();

		console.error('distributed parallelize collect result')
		console.error(res);

		//compare a and res
		test = true;
		for (var i = 0; i < res.length; i++) {
			if (res[i] != a[i]) {
				test = false;
				break;
			}				
		}
		var endTime = new Date();
		if (test) {
			console.log('tesk ok');
			process.exit(0);
		}		
		else {
			console.log('tesk ko');
			process.exit(1);
		}
		grid.disconnect();
	})();
} catch (err) {
	process.exit(2);
}

