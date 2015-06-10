#!/usr/local/bin/node --harmony

// Test randomSVMData -> flatMapValues -> lookup

var co = require('co');
var ugrid = require('../..');
var test = require('../ugrid-test.js');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var N = 5, D = 1, seed = 1, key = -1;

	function mapper(e) {
		var out = [];
		for (var i = e; i <= 5; i++)
			out.push(i);
		return out;
	}

	var ref = test.randomSVMData(N, D, seed);
	ref = test.flatMapValues(ref, mapper).filter(function (e) {return e[0] == key;});
	var res = yield uc.randomSVMData(N, D, seed).flatMapValues(mapper).lookup(key);
	console.assert(test.arrayEqual(ref.sort(), res.sort()));

	uc.end();
}).catch(ugrid.onError);
