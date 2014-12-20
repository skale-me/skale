#!/usr/local/bin/node --harmony

// this programme checks every second the server 
// to know if all workers have been connected to the server
// then we run all the unit tests
var co = require('co');
var thunkify = require('thunkify');
var fs = require('fs');
var UgridClient = require('../lib/ugrid-client.js');
var spawn = require('child_process').spawn;
var yieldable_readdir = thunkify(fs.readdir);   
var nPassedTests = 0;
var nFailedTests = 0;

var grid = new UgridClient({
	host: 'localhost',
	port: 12346
});

co(function*(){
	var res = yield grid.connect();
	var N = process.argv[2] || 1;
	// Query devices, get uuid array
	var t0 = setInterval(function() {
		grid.devices_cb({type: 'worker'}, function (err, res) {
			if (err) 
				throw err;
			if (res.length != N)
				throw 'Not enough workers to run tests';
			else {
				clearInterval(t0);
				runTests(grid);
			}
		});
	}, 1000);
})();

function runTests(grid) {
	function spawnTest_cb(file, callback) {
		var result;
		var prog = spawn('node', ['--harmony', file]); // run in a asynchronous way all the unit tests
		var startTime = new Date();
		prog.on('exit', function (code) {
			var endTime = new Date();
			runtime = (endTime - startTime) / 1000;
			if (code) {
				nFailedTests++;
				console.log(file + ' FAILED');
			} else
				nPassedTests++;
			callback(null, {status : code, time : runtime}); //return the result
		});
	}
	var spawnTest = thunkify(spawnTest_cb);

	co(function*() {
		var testResults = {};
		var files = yield yieldable_readdir('unitTest/'); //make sur that we read all the files  
		console.log('Found ' + files.length + ' unit tests');		
		for (var i = 0; i < files.length; i++) {
			var res = yield spawnTest('unitTest/' + files[i]); 
			testResults[files[i]] = res;
		}
		console.log(nPassedTests + ' passed');
		console.log(nFailedTests + ' failed');
		// fs.writeFile('results.json', JSON.stringify(testResults, null, '\t'), function(err) {
		// 	if (err) throw err;
			grid.disconnect();
		// });
	})();
}	
