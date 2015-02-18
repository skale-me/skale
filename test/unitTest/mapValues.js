#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

var a = [[0, 1], [1, 2], [1, 3]];

co(function *() {
	yield ugrid.init();

	function by2 (e) {
		return e * 2;
	}

	var p1 = ugrid.parallelize(a).mapValues(by2);

	console.log(yield p1.collect());	

	ugrid.end();
})();
