#!/usr/local/bin/node --harmony

// parallelize -> map -> lookup

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var key = 1;
	var value = 2;
	var v = [[key, value], [3, 4], [5, 6]];
	var loc = JSON.parse(JSON.stringify(v)).map(by2).filter(function(e){return (e[0] == key)});

	function by2 (e) {
		e[1] *= 2;
		return e;
	}

	var dist = yield uc.parallelize(v).map(by2).lookup(key);

	console.log(loc)
	console.log(dist)

	console.assert(JSON.stringify(dist) == JSON.stringify(loc))

	uc.end();
}).catch(ugrid.onError);
