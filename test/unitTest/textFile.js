#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var readline = require('readline');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	// Create test file
	var file = '/tmp/textFile.txt';
	var a = '-1 1 1 1 1 1 1 1 1 1 1\n' +
		'-1 2 2 2 2 2 2 2 2 2 2\n' +
		'-1 3 3 3 3 3 3 3 3 3 3\n' +
		'-1 4 4 4 4 4 4 4 4 4 4';
	fs.writeFileSync(file, a);

	// Distributed read
	var res = yield uc.textFile(file).collect();

	// Local read
	var V = [];
	var rl = readline.createInterface({input: fs.createReadStream(file), output: process.stdout, terminal: false});
	rl.on("line", function (line) {V.push(line);});

	rl.on('close', function () {
		fs.unlink(file, function () {
			console.log(res);
			console.log(V);
			console.assert(V.length == res.length);

			for (var i = 0; i < V.length; i++)
				if (V[i] != res[i])
					throw new Error('error: local and distributed array have different elements');
			uc.end();

		});
	});
}).catch(ugrid.onError);
