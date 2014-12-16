#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../lib/ugrid-ml.js');

co(function *() {
	yield ugrid.init();
	
	var res = yield ugrid.textFile('test/svm_data_sample.txt').collect();

	console.log(res);

	ugrid.end();
})();