#!/usr/local/bin/node --harmony

var co = require('co');
var ugrid = require('../../');

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function dup (e) {
		return [e, e];
	}

	var v = [1, 2, 3, 4, 5];
	var v_copy = JSON.parse(JSON.stringify(v));
	var data = uc.parallelize(v).flatMap(dup).persist();
	var res = yield data.count();

	v.push(6);
	var res = yield data.count();

	var tmp = v_copy.map(dup).reduce(function (a, b) {return a.concat(b);}, []);

	console.assert(tmp.length == res);

	uc.end();
}).catch(ugrid.onError);
