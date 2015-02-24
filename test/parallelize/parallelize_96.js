#!/usr/local/bin/node --harmony

// parallelize -> persist -> join -> count
// parallelize -> persist ->

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
// var join = require('../ugrid-test.js').join;

co(function *() {
	yield ugrid.init();

	var v1 = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]];
	var v2 = [[0, 5], [1, 6], [2, 7], [3, 8], [4, 9]];
	// var loc = union(v1, v2);

	var d1 = ugrid.parallelize(v1);
	var d2 = ugrid.parallelize(v2);

	// var dist = yield d1.join(d2).collect();

	// console.log(dist)

	// loc = loc.sort();
	// dist = dist.sort();
	// for (var i = 0; i < loc.length; i++)
	// 	console.assert(loc[i] == dist[i]);

	throw 'this test is not finished'

	ugrid.end();
})();
