#!/usr/local/bin/node --harmony

var co = require('co');
var UgridClient = require('../../lib/ugrid-client.js');
var UgridContext = require('../../lib/ugrid-context.js');
var ml = require('../../lib/ugrid-ml.js');
var fs = require('fs');

var M = 5;
var a = ml.randn(M);

function doubles(n) {
	return n * 2;
}

var b = a.map(doubles);
var grid = new UgridClient({host: 'localhost', port: 12346, data: {type: 'master'}});
var name = 'map';
var json = {
	succes: false,
	time: 0,
	error:'none'
}
co(function *() {
	try {
		
		var startTime = new Date();
		yield grid.connect();
		var res = yield grid.send('devices', {type: "worker"});
		var ugrid = new UgridContext(grid, res.devices);
		
		var res = yield ugrid.parallelize(a).map(doubles, []).collect();

		console.error('distributed map result')
		console.log(res);

		console.error('\nlocal map result')
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
		//~ fs.writeFile(name + '.json', JSON.stringify(json, null, '\t'));
		grid.disconnect();
	}
	catch (err){
		json.error = err;
		console.log(JSON.stringify(json));
		process.exit(1);
	}	
})();

