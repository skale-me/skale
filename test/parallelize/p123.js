#!/usr/local/bin/node --harmony

// parallelize -> flatMapValues -> collect

var co = require('co');
var ugrid = require('../../');
var flatMapValues = require('../ugrid-test.js').flatMapValues;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[1, 2], [3, 4], [3, 6]];

	function mapper(e) {
		var out = [];
		for (var i = e; i <= 5; i++)
			out.push(i);
		return out;
	}

	var loc = flatMapValues(v, mapper);
	var dist = yield uc.parallelize(v).flatMapValues(mapper).collect();

	loc = loc.sort();
	dist = dist.sort();

	console.assert(JSON.stringify(loc) == JSON.stringify(dist));

	uc.end();
}).catch(ugrid.onError);
