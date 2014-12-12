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

var grid = new UgridClient({
	host: 'localhost',
	port: 12346
});

co(function*(){
	var res = yield grid.connect();
	var N = process.argv[2] || 5;
	// Query devices, get uuid array
	var t0 = setInterval(function() {
		grid.send_cb({cmd: 'devices', data: {type: 'worker'}}, function(err, res) {
			if (err) console.log(err);
			console.log(res);
			if (res.length != N)
				console.log(res.length + ' workers connected');
			else {
				console.log('Cluster ready: ' + N + ' worker(s) connected')
				clearInterval(t0);
				runTests(grid);
			}
		})
	}, 1000);
})();

function runTests(grid) {
	function spawnTest_cb(file, callback) {
		var result;
		var prog = spawn('node', ['--harmony', file]); // run in a asynchronous way all the unit tests
		prog.stdout.on('data', function (data) {
			try {result = JSON.parse(data);}
			catch (error) {throw 'Coprocess is returning a bad string format'}
		});

		prog.on('close', function (code) {
			// test finished
			callback(null, result); //return the result 
		});
	}
	var spawnTest = thunkify(spawnTest_cb);

	co(function*() {
		var testResults = {};
		var files = yield yieldable_readdir('unitTest/'); //make sur that we read all the files  
		for (var i = 0; i < files.length; i++) {
			console.log('running : ' + files[i]);
			var res = yield spawnTest('unitTest/' + files[i]); 
			testResults[files[i]] = res || 'Bad test file';
		}
		console.log('\n=> Test results: ')
		console.log(testResults);
		fs.writeFile('results.json', JSON.stringify(testResults, null, '\t'), function(err) {
			if (err) throw err;
			grid.disconnect();
			process.exit(0);
		});
	})();
}	
