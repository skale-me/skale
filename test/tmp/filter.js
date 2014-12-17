#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var a = ml.randn(M);

function positive(n) {
	return (n > 0) ? true : false;
}

var b = a.filter(positive);

var json = {success: false, time: 0};

try {
	co(function *() {
		var startTime = new Date();
		yield ugrid.init();
		
		console.error('a = ')
		console.error(a)
		console.error('b = ')
		console.error(b)
		var res = yield ugrid.parallelize(a).filter(positive, []).collect();

		console.error('distributed filter result')
		console.error(res);

		console.error('\nlocal filter result')
		console.error(b);
		//compare b and res
		test = true;
		for (var i = 0; i < res.length; i++) {
			if (res[i] != b[i]) {
				test = false;
				break;
			}
		}
		var endTime = new Date();
		if (test) {
			// json with test results
			console.log("test ok");
			process.exit(0); //test OK
		} else {
			console.log("test ko");
			process.exit(1); //test KO
		}
		ugrid.end();
	})();
} catch (err) {
	console.log(" #######  ERRORRR ")
	process.exit(2);
}

