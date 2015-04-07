#!/usr/local/bin/node --harmony

// parallelize -> reduceByKey -> persist -> lookup

var co = require('co');
var ugrid = require('../../');
var reduceByKey = require('../ugrid-test.js').reduceByKey;

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [[0, 1], [0, 2], [1, 3], [2, 4]];
	var key = 0;

	function reducerByKey(a, b) {
		a += b;
		return a;
	}

	var loc = reduceByKey(v, reducerByKey, 0).filter(function(e) {return (e[0] == key)});

	var data = uc.parallelize(v).reduceByKey(reducerByKey, 0).persist();
	yield data.count();

	v.push([key, 10]);
	var dist = yield data.lookup(key);

	loc = loc.sort();
	dist = dist.sort();

	for (var i = 0; i < loc.length; i++)
		for (var j = 0; j < loc[i].length; j++)
			console.assert(loc[i][j] == dist[i][j]);

	uc.end();
}).catch(ugrid.onError);
