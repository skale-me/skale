#!/usr/local/bin/node --harmony

var co = require('co');
var thunkify = require('thunkify');
var fs = require('fs');
var spawn = require('child_process').spawn;
var yieldable_readdir = thunkify(fs.readdir);   
var nPassedTests = 0, nFailedTests = 0, nIgnored = 0;

var test_dir = process.env['UNIT_TEST_DIR'] || 'unitTest';

co(function*(){
	function spawnTest_cb(file, callback) {
		var result;
		var prog = spawn('node', ['--harmony', file]); // run in a asynchronous way all the unit tests
		var startTime = new Date();
		process.stdout.write("\r" + file + "                                 ");
		prog.on('exit', function (code) {
			var endTime = new Date();
			runtime = (endTime - startTime) / 1000;
			if (code) {
				nFailedTests++;
				console.log();
				console.log(file + ' FAILED');
			} else
				nPassedTests++;
			callback(null, {status : code, time : runtime}); //return the result
		});
	}
	var spawnTest = thunkify(spawnTest_cb);

	var testResults = {};
	var files = yield yieldable_readdir(test_dir + '/'); //make sur that we read all the files  

	console.log('Found ' + files.length + ' unit tests');
	for (var i = 0; i < files.length; i++) {
		if (!files[i].match(/\.js$/)) {
			nIgnored++;
			continue
		}
		testResults[files[i]] = yield spawnTest(test_dir + '/' + files[i]); 
	}
	console.log("\r                                                        ");
	console.log(nIgnored + ' ignored');
	console.log(nPassedTests + ' passed');
	console.log(nFailedTests + ' failed');
})();
