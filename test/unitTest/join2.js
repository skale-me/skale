#!/usr/local/bin/node --harmony
'use strict';

// Join compute the cartesian product between a and b entries presenting same key
// cogroup build a sequence of value for the same key
// thus join output can present different entries with same key
// which is not the case for cogroup

var co = require('co');
var assert = require('assert');
var ugrid = require('../..');

var a = [[0, 'hello'], [1, 'goodbye'], [1, 'TEST']];
var b = [[0, 'cedric'], [1, 'marc']];

co(function *() {
	var uc = yield ugrid.context();
	console.assert(uc.worker.length > 0);

	var p1 = uc.parallelize(a);
	var p2 = uc.parallelize(b);
	var p3 = p1.join(p2);

	// console.log(yield p1.collect());
	// console.log(yield p2.collect());
	console.log(yield p3.collect());	

	uc.end();
}).catch(ugrid.onError);
