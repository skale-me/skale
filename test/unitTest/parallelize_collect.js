#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var a = ml.randn(M);
console.error(a);

var name = 'parallelize_collect';

try {
	co(function *() {
		var startTime = new Date();
		yield ugrid.init();
		
		var res = yield ugrid.parallelize(a).collect();

		console.error('distributed parallelize collect result')
		console.error(res);

		//compare a and res
		test = true;
		for (var i = 0; i < res.length; i++) {
			if (res[i] != a[i]) {
				test = false;
				break;
			}				
		}
		var endTime = new Date();
		if (test) {
			console.log('tesk ok');
			process.exit(0);
		}		
		else {
			console.log('tesk ko');
			process.exit(1);
		}
		ugrid.end();
	})();
} catch (err) {
	process.exit(2);
}

