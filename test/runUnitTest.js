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
	console.log('Logged with uuid: ' + res.uuid);
	var N = process.argv[2] || 5;
	// Query devices, get uuid array
	var t0 = setInterval(function() {
		grid.send_cb('devices', {type: "worker"}, function(err, res) {
			if (err) console.log(err);
			if (res.devices.length != N)
				console.log(res.devices.length + ' workers connected');
			else {
				console.log('Cluster ready: ' + N + ' worker(s) connected')
				clearInterval(t0);
				runTests(grid);
			}
		})
	}, 1000);
})();

function runTests(grid) {
	try{
		function spawnTest_cb(file, callback) {
			var result;
			var prog = spawn('node', ['--harmony', file]); // run in a asynchronous way all the unit tests
			prog.stdout.on('data', function (data) {
				//capture the stdout of each unit test
				result = JSON.parse(data);
			});
			prog.on('close', function (code) {
				//test finished
				callback(null, result); //return the result 
			});
		}
		var spawnTest = thunkify(spawnTest_cb);

		co(function*() {
			var testResults = {};
			var files = yield yieldable_readdir('../test/unitTest/'); //make sur that we read all the files  
			for (var i = 0; i < files.length; i++) {
				console.log('running : ' + files[i]);
				var res = yield spawnTest('../test/unitTest/' + files[i]); 
				testResults[files[i]] = res;
			}
			console.log(testResults);
			fs.writeFile('results.json', JSON.stringify(testResults, null, '\t'));
		})();
		
		grid.disconnect();
	}
	catch (err){
		console.log("ERROR: " + err);
	}	
}	

