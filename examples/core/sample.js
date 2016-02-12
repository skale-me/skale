#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

uc.parallelize([1, 2, 3, 4]).sample(false, 0.8).collect().toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([1, 2, 3, 4]));	
	console.log('Success !');
	console.log(res);
	uc.end();
})
