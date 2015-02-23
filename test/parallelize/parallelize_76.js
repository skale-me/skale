#!/usr/local/bin/node --harmony

// parallelize -> persist -> reduceByKey -> count();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var reduceByKey = require('..//ugrid-test.js').reduceByKey;

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	function reducer(a, b) {		
		a += b;
		return a;
	}

	var loc = reduceByKey(v, reducer, 0).length;

	var data = ugrid.parallelize(v).persist();
	yield data.count();

	v.push([0, 10]);
	var dist = yield data.reduceByKey(reducer, 0).count();

	console.assert(loc == dist);

	ugrid.end();
})();
