#!/usr/local/bin/node --harmony

// parallelize -> persist -> groupByKey -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var groupByKey = require('..//ugrid-test.js').groupByKey;

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var loc = groupByKey(v).length;

	var data = ugrid.parallelize(v).persist();
	yield data.count();

	v.push([0, 11]);
	var dist = yield data.groupByKey().count();

	console.assert(loc == dist);

	ugrid.end();
})();
