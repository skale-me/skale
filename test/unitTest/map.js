#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../../lib/ugrid-context.js')({host: 'localhost', port: 12346});
var ml = require('../../lib/ugrid-ml.js');

var M = 5;
var a = ml.randn(M);

function doubles(n) {
	return n * 2;
}

var b = a.map(doubles);

try {
	co(function *() {
		yield ugrid.init();

		var res = yield ugrid.parallelize(a).map(doubles, []).collect();

		console.error('distributed map result')
		console.error(res);

		console.error('\nlocal map result')
		console.error(b);
		//compare b and res
		test = true;
		for (var i = 0; i < res.length; i++) {
			if (res[i] != b[i]) {
				test = false;
				break;
			}
		}
		if (test) {
			console.log('tesk ok');
			process.exit(0); //test OK
		} else {
			console.log('tesk ko');
			process.exit(1); //test KO	
		}
		ugrid.end();
	})();
} catch (err) {
	process.exit(2); //error in test test
}
