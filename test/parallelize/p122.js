#!/usr/local/bin/node --harmony

// parallelize -> flatMapValues -> count

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var flatMapValues = require('../ugrid-test.js').flatMapValues;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[1, 2], [3, 4], [3, 6]];

	function mapper(e) {
		var out = [];
		for (var i = e; i <= 5; i++)
			out.push(i);
		return out;
	}

	var loc = flatMapValues(v, mapper);
	var dist = yield ugrid.parallelize(v).flatMapValues(mapper).count();

	console.assert(loc.length == dist)

	ugrid.end();
})();
