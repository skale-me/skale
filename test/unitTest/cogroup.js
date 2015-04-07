#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

var a = [[0, 'hello'], [1, 'goodbye'], [1, 'TEST']];
var b = [[0, 'cedric'], [1, 'marc']];

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var p1 = uc.parallelize(a);
	var p2 = uc.parallelize(b);
	var p3 = p1.coGroup(p2);

	console.log(yield p3.collect());

	uc.end();
}).catch(ugrid.onError);
