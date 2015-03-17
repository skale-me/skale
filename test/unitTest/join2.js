#!/usr/local/bin/node --harmony
'use strict';

// Join compute the cartesian product between a and b entries presenting same key
// cogroup build a sequence of value for the same key
// thus join output can present different entries with same key
// which is not the case for cogroup

var co = require('co');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();

process.on("exit", function () {console.assert(ugrid.grid.id !== undefined);});

var a = [[0, 'hello'], [1, 'goodbye'], [1, 'TEST']];
var b = [[0, 'cedric'], [1, 'marc']];

co(function *() {
	yield ugrid.init();

	var p1 = ugrid.parallelize(a);
	var p2 = ugrid.parallelize(b);
	var p3 = p1.join(p2);

	// console.log(yield p1.collect());
	// console.log(yield p2.collect());
	console.log(yield p3.collect());	

	ugrid.end();
}).catch(function (err) {
	console.error(err.stack);
	process.exit(1);
});
