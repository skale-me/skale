#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');
var tl = require('./test-lib.js');
var fs = require('fs');

var M = 5;
var a = ml.randn(M);

var b = a.filter(tl.positive);
var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});
var name = 'filter';
var json = {
	succes: false,
	time: 0,
	error:'none'
}
co(function *() {
	try 
	{
		var startTime = new Date();
		yield grid.connect();
		var res = yield grid.send('devices', {type: "worker"});
		var ugrid = new UgridContext(grid, res.devices);
		
		console.error('a = ')
		console.error(a)
		console.error('b = ')
		console.error(b)
		var res = yield ugrid.parallelize(a).filter(tl.positive, []).collect();
		
		console.error('distributed filter result')
		console.log(res);

		console.error('\nlocal filter result')
		console.log(b);
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
	catch (err){
		json.error = err;
		console.log(JSON.stringify(json));
		process.exit(1);
	}
})();

