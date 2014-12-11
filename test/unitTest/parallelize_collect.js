#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');
var fs = require('fs');

var M = 5;
var a = ml.randn(M);

console.log(a)
var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});
var name = 'parallelize_collect';
var json = {
	succes: false,
	time: 0,
	error : "none"
}
co(function *() {
	
	try {
		
		var startTime = new Date();
		yield grid.connect();
		var res = yield grid.send('devices', {type: "worker"});
		var ugrid = new UgridContext(grid, res.devices);
		
		var res = yield ugrid.parallelize(a).collect();

		console.error('distributed parallelize collect result')
		console.log(res);

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
			// json with test results
			json.succes = true;
			json.time = (endTime - startTime) / 1000;
			console.error('\ntest ok');
		}		
		else {
			json.succes = false;
			json.time = (endTime - startTime) / 1000;
			console.error('\ntest ko');
		}
		console.log(JSON.stringify(json));
		grid.disconnect();
	}
	catch (err) {
		json.error = err;
		console.log(JSON.stringify(json));
		process.exit(1);
	}
})();

