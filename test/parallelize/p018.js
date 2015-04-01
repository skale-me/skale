#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var v = [1, 2, 3, 4, 5];

	function dup(e) {
		return [e, e];
	}

	function sum(a, b) {
		a += b;
		return a;
	}	

	var res = yield uc.parallelize(v).flatMap(dup).reduce(sum, 0);

	var tmp = v.map(dup).reduce(function(a, b) {return a.concat(b)}, []).reduce(sum, 0);
	
	assert(tmp == res);

	uc.end();
}).catch(ugrid.onError);
