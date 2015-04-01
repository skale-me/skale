#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> count();

var co = require('co');
var ugrid = require('../../');
var reduceByKey = require('../ugrid-test.js').reduceByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	function reducer(a, b) {		
		a += b;
		return a;
	}

	var loc = reduceByKey(v, reducer, 0).length;
	var dist = yield uc.parallelize(v).reduceByKey(reducer, 0).count();

	console.assert(loc == dist);

	uc.end();
}).catch(ugrid.onError);
