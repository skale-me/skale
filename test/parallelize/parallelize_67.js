#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> lookup();

var co = require('co');
var ugrid = require('../../lib/ugrid-context.js')();
var reduceByKey = require('..//ugrid-test.js').reduceByKey;

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

co(function *() {
	yield ugrid.init();

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var key = 0;

	function reducerByKey(a, b) {
		a += b;
		return a;
	}

	var loc = reduceByKey(v, reducerByKey, 0).filter(function(e) {return (e[0] == key)});
	var dist = yield ugrid.parallelize(v).reduceByKey(reducerByKey, 0).lookup(key);

	loc = loc.sort();
	dist = dist.sort();

	for (var i = 0; i < loc.length; i++)
		for (var j = 0; j < loc[i].length; j++)
			console.assert(loc[i][j] == dist[i][j]);

	ugrid.end();
})();
