#!/usr/local/bin/node --harmony

// hdfsTextFile -> collect

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	// var v = [1, 2, 3, 4, 5];
	// var t0 = v.reduce(function(a, b) {return a + b + '\n'}, '');
	// fs.writeFileSync('/tmp/v', t0);

	// var loc = v.map(String);
	// var dist = yield ugrid.textFile('/tmp/v').collect();

	// console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	var dist = yield ugrid.textFile('hdfs://localhost:9000/svm200MB').count();

	// var dist = yield ugrid.textFile('hdfs://localhost:9000/v').count();

	console.log(dist);

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
