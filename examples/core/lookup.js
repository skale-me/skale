#!/usr/bin/env node

var assert = require('assert');
var sc = require('skale').context();

sc.parallelize([[1,2],[3,4],[3,6]]).
  lookup(3).
  toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify([4, 6]));  
	console.log('Success !') //expected [ 4, 6 ]
	console.log(res);
	sc.end();
})
