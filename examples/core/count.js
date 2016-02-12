#!/usr/bin/env node

var assert = require('assert');
var uc = new require('ugrid').Context();

uc.parallelize([1, 2, 3, 4]).count(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify(4));
	console.log('Success !')
	console.log(res);
	uc.end();
});
