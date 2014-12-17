#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var readline = require('readline');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});

co(function *() {
	yield ugrid.init();

	// Distributed read
	var file = 'test/svm_data_sample.txt';
	var P = process.argv[2];
	var res = yield ugrid.textFile(file, P).collect();

	// Local read
	var V = [];
	var path = 'svm_data_sample.txt';		// When running test with npm test command
	// var path = '../svm_data_sample.txt'; // When running test manually
	var rl = readline.createInterface({input: fs.createReadStream(path), output: process.stdout, terminal: false});
	rl.on("line", function (line) {V.push(line);});

	rl.on('close', function(err) {
		if (V.length != res.length)
			throw 'error: local and distributed array have different lengths';

		for (var i = 0; i < V.length; i++)
			if (V[i] != res[i])
				throw 'error: local and distributed array have different elements';
		ugrid.end();
	})
})();