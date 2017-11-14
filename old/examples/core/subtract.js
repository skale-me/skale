#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

var d1 = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];
var d2 = [[1, 1], [1, 1], [2, 3]];

uc.parallelize(d1).subtract(uc.parallelize(d2)).collect().toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([[3, 5], [2, 4]])); 	
	console.log('Success !')
	console.log(res);
	uc.end();
});
