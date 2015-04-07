#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> reduce();

var co = require('co');
var ugrid = require('../../');
var reduceByKey = require('../ugrid-test.js').reduceByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	function reducerByKey(a, b) {
		a += b;
		return a;
	}

	// sum keys and values
	function reducer(a, b) {
		a[0] += b[0];
		a[1] += b[1];
		return a;
	}

	var loc = reduceByKey(v, reducerByKey, 0).reduce(reducer, [0, 0]);
	var dist = yield uc.parallelize(v).reduceByKey(reducerByKey, 0).reduce(reducer, [0, 0]);

	console.assert(loc[0] == dist[0]);
	console.assert(loc[1] == dist[1]);	

	uc.end();
}).catch(ugrid.onError);
