#!/usr/bin/env node

var assert = require('assert');
var sc = require('skale').context();

sc.parallelize([[1,2],[2,4],[4,6]]).
  keys().
  collect().
  toArray(function(err, res) {
	res.sort();
	assert(JSON.stringify(res) === JSON.stringify([1,2,4]));	  
	console.log('Success !') //expected = [1,2,4]
	console.log(res);
	sc.end();
})
