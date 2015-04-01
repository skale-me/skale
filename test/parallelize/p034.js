#!/usr/local/bin/node --harmony

var co = require('co');
var assert = require('assert');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function dup (e) {
		return [e, e];
	}	

	function sum(a, b) {
		a += b;
		return a;
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));

	var data = uc.parallelize(v).persist();
	yield data.reduce(sum, 0);

	v.push(6);
	var res = yield data.flatMap(dup).reduce(sum, 0);

	var tmp = v_copy.map(dup)
		.reduce(function (a, b) {return a.concat(b);}, [])
		.reduce(sum, 0);

	assert(res == tmp);

	uc.end();
}).catch(ugrid.onError);
