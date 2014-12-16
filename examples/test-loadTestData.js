#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../lib/ugrid-context.js')({host: 'localhost', port: 12346});

co(function *() {
	yield ugrid.init();

	var N = 4;
	var D = 2;

	// Section 1: without persist()
	var t0 = ugrid.loadTestData(N, D);
	var r1 = yield t0.collect();
	var r2 = yield t0.collect();

	var passed = true;
	top:
	for (var i = 0; i < N; i++) {
		for (var j = 0; j < D; j++) {
			if (r1[i].features[j] != r2[i].features[j]) {
				passed = false;
				break top;
			}
		}
	}

	console.log(passed ? 'Test without persist PASSED' : 'Test without persist FAILED');

	// Section 2: with persist()
	var t0 = ugrid.loadTestData(N, D).persist();
	var r1 = yield t0.collect();
	var r2 = yield t0.collect();

	var passed = true;
	top:
	for (var i = 0; i < N; i++) {
		for (var j = 0; j < D; j++) {
			if (r1[i].features[j] != r2[i].features[j]) {
				passed = false;
				break top;
			}
		}
	}

	console.log(passed ? 'Test with persist PASSED' : 'Test with persist FAILED');

	ugrid.end();
})();
