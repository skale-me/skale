#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var a = ml.randn(M);

function positive(n) {
	return (n > 0) ? true : false;
}

var b = a.filter(positive);

var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});
var json = {success: false, time: 0};

try {
	co(function *() {
		var startTime = new Date();
		yield grid.connect();
		var devices = yield grid.send({cmd: 'devices', data: {type: "worker"}});
		var ugrid = new UgridContext(grid, devices);
		
		console.error('a = ')
		console.error(a)
		console.error('b = ')
		console.error(b)
		var res = yield ugrid.parallelize(a).filter(positive, []).collect();

		console.error('distributed filter result')
		console.error(res);

		console.error('\nlocal filter result')
		console.error(b);
		//compare b and res
		test = true;
		for (var i = 0; i < res.length; i++) {
			if (res[i] != b[i]) {
				test = false;
				break;
			}
		}
		var endTime = new Date();
		if (test) {
			// json with test results
			json.success = true;
			json.time = (endTime - startTime) / 1000;
			console.error('\ntest ok');
		} else {
			json.success = false;
			json.time = (endTime - startTime) / 1000;
			console.error('\ntest ko');
		}
		console.log(JSON.stringify(json));
		grid.disconnect();
	})();
} catch (err) {
	json.error = err;
	console.log(JSON.stringify(json));
	process.exit(1);
}

