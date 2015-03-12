#!/usr/local/bin/node --harmony
'use strict';

// Join compute the cartesian product between a and b entries presenting same key
// cogroup build a sequence of value for the same key
// thus join output can present different entries with same key
// which is not the case for cogroup

var co = require('co');
var fs = require('fs');
var assert = require('assert');
var ugrid = require('../../lib/ugrid-context.js')();
var ml = require('../../lib/ugrid-ml.js');

var file1 = '/tmp/data.txt';
fs.writeFileSync(file1, '0 world\n1 monde');

var file2 = '/tmp/data2.txt';
fs.writeFileSync(file2, '0 cedric\n1 cedric');

co(function *() {
	yield ugrid.init();

	function parse(e) {return e.split(' ');}

	var p1 = ugrid.textFile(file1).map(parse);
	var p2 = ugrid.textFile(file2).map(parse);	
	var p3 = p1.join(p2);

	// console.log(yield p1.collect());
	// console.log(yield p2.collect());
	console.log(yield p3.collect());	

	fs.unlink(file1, function (err) {
		fs.unlink(file2, function (err) {
			ugrid.end();
		});
	});
})();
