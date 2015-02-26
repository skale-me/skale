#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

var a = [[0, 'hello'], [1, 'goodbye'], [1, 'TEST']];
var b = [[0, 'cedric'], [1, 'marc']];

co(function *() {
	yield ugrid.init();

	var p1 = ugrid.parallelize(a);
	var p2 = ugrid.parallelize(b);
	var p3 = p1.coGroup(p2);

	// console.log(yield p1.collect());
	// console.log(yield p2.collect());
	console.log(yield p3.collect());

	ugrid.end();
})();
