#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

var a = [[0, 1], [1, 2], [1, 3]];

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	function by2 (e) {
		return e * 2;
	}

	var p1 = uc.parallelize(a).mapValues(by2);

	console.log(yield p1.collect());	

	uc.end();
}).catch(ugrid.onError);
