#!/usr/local/bin/node --harmony

// parallelize -> persist -> reduce

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);
	
	function sum(a, b) {
		a += b;
		return a;
	}

	var v = [1, 2, 3, 4, 5];

	var loc = v.reduce(sum, 0);
	var data = uc.parallelize(v).persist();
	yield data.reduce(sum, 0);
	v.push(6);
	var dist = yield data.reduce(sum, 0);

	console.assert(dist == loc);

	uc.end();
}).catch(ugrid.onError);
