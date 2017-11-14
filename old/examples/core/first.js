#!/usr/bin/env node

var assert = require('assert');
var uc = require('ugrid').context();

uc.parallelize([[1,2],[3,4],[3,6]]).
  first().
  toArray(function(err, res) {
	res.sort();
	assert(JSON.stringify(res) === JSON.stringify([[1, 2]]));		  
	console.log('Success !') //expected [ [ 1, 2 ] ]
	console.log(res);
	uc.end();
})
