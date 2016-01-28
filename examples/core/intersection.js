#!/usr/bin/env node

var assert = require('assert');
var sc = require('skale').context();

sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9]).
  intersection(sc.parallelize([5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])).
  collect().
  toArray(function(err, res) {
	res.sort();
	assert(JSON.stringify(res) === JSON.stringify([5, 6, 7, 8, 9])); 	  
	console.log('Success !')  //expected = [5,6,7,8,9]
	console.log(res);
	sc.end();
})
