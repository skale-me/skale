#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var a = ml.randn(M);

function doubles(n) {
	return n * 2;
}

var b = a.map(doubles);
var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});

try {
	co(function *() {
		yield grid.connect();
		var devices = yield grid.send({cmd: 'devices', data: {type: "worker"}});
		var ugrid = new UgridContext(grid, devices);

		var res = yield ugrid.parallelize(a).map(doubles, []).collect();

		console.error('distributed map result')
		console.error(res);

		console.error('\nlocal map result')
		console.error(b);
		//compare b and res
		test = true;
		for (var i = 0; i < res.length; i++) {
			if (res[i] != b[i]) {
				test = false;
				break;
			}
		}
		if (test) {
			console.log('tesk ok');
			process.exit(0); //test OK
		} else {
			console.log('tesk ko');
			process.exit(1); //test KO	
		}
		grid.disconnect();
	})();
} catch (err) {
	process.exit(2); //error in test test
}
