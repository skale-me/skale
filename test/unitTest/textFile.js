#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var readline = require('readline');
var ugrid = require('../../lib/ugrid-context.js')();

co(function *() {
	yield ugrid.init();

	// Create test file
	var file = '/tmp/textFile.txt';
	var a = '-1 1 1 1 1 1 1 1 1 1 1\n' +
		'-1 2 2 2 2 2 2 2 2 2 2\n' +
		'-1 3 3 3 3 3 3 3 3 3 3\n' +
		'-1 4 4 4 4 4 4 4 4 4 4';
	fs.writeFileSync(file, a);

	// Distributed read
	var P = process.argv[2];
	var res = yield ugrid.textFile(file, P).collect();

	// Local read
	var V = [];
	var rl = readline.createInterface({input: fs.createReadStream(file), output: process.stdout, terminal: false});
	rl.on("line", function (line) {V.push(line);});

	rl.on('close', function () {
		if (V.length != res.length)
			throw 'error: local and distributed array have different lengths';

		for (var i = 0; i < V.length; i++)
			if (V[i] != res[i])
				throw 'error: local and distributed array have different elements';
			fs.unlink(file);
		ugrid.end();
	});
})();
