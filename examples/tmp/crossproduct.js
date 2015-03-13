#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

var a = [0, 1];
var b = [2, 3];

co(function *() {
	yield ugrid.init();

	var p1 = ugrid.parallelize(a);
	var p2 = ugrid.parallelize(b);
	var p3 = p1.crossProduct(p2);

	console.log(yield p3.collect());	

	ugrid.end();
})();
