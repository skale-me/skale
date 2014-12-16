#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var v = ml.randn(M);
//console.error(v);

try {
	co(function *() {
		var startTime = new Date();
		yield ugrid.init();
		var a = ugrid.parallelize(v).persist();
		var r1 = yield a.collect();
		console.log(r1);
		var r2 = yield a.collect();
		console.error('distributed parallelize collect result')
		console.error(r1);
		console.error(r2);

		//compare r1 and r2
		test = true;
		for (var i = 0; i < r1.length; i++) {
			if (r1[i] != r2[i]) {
				test = false;
				break;
			}				
		}
		var endTime = new Date();
		if (test) {
			console.log('tesk ok');
			process.exit(0);
		} else {
			console.log('tesk ko');
			process.exit(1);
		}
		ugrid.end();
	})();
} catch (err) {
	process.exit(2);
}
