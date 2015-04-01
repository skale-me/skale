#!/usr/local/bin/node --harmony

// hdfsTextFile -> collect

var co = require('co');
var ugrid = require('../..');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	// var v = [1, 2, 3, 4, 5];
	// var t0 = v.reduce(function(a, b) {return a + b + '\n'}, '');
	// fs.writeFileSync('/tmp/v', t0);

	// var loc = v.map(String);
	// var dist = yield uc.textFile('/tmp/v').collect();

	// console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	var dist = yield uc.textFile('hdfs://localhost:9000/v').collect();

	console.log(dist);

	uc.end();
}).catch(ugrid.onError);
