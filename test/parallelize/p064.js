#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> count();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var reduceByKey = require('..//ugrid-test.js').reduceByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];

	function reducer(a, b) {		
		a += b;
		return a;
	}

	var loc = reduceByKey(v, reducer, 0).length;
	var dist = yield ugrid.parallelize(v).reduceByKey(reducer, 0).count();

	console.assert(loc == dist);

	ugrid.end();
})();
