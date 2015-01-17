#!/usr/local/bin/node --harmony

var co = require('co');
var thunkify = require('thunkify');
var fs = require('fs');
var UgridClient = require('../lib/ugrid-client.js');
var spawn = require('child_process').spawn;
var yieldable_readdir = thunkify(fs.readdir);   
var nPassedTests = 0, nFailedTests = 0;

var grid = new UgridClient({host: 'localhost', port: 12346});

co(function*(){
	var res = yield grid.connect();

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

	var testResults = {};
	var files = yield yieldable_readdir('unitTest/'); //make sur that we read all the files  
	console.log('Found ' + files.length + ' unit tests');		
	for (var i = 0; i < files.length; i++) {
		var res = yield spawnTest('unitTest/' + files[i]); 
		testResults[files[i]] = res;
	}
	console.log(nPassedTests + ' passed');
	console.log(nFailedTests + ' failed');
	grid.disconnect();
})();
