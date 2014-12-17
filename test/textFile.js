#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();

	var file = 'test/svm_data_sample.txt';
	var P = process.argv[2];

	var res = yield ugrid.textFile(file, P).collect();

	console.log(res);

	ugrid.end();
})();