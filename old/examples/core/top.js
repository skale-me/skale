#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

uc.parallelize([1, 2, 3, 4], 2).top(2).toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([3, 4])); 	
	console.log('Success !')
	console.log(res);
	uc.end();
})
