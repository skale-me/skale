#!/usr/bin/env node

var uc = require('ugrid').context();

uc.parallelize([[1,2],[2,4],[4,6]]).
  keys().
  collect().
  toArray(function(err, res) {
	console.log('Success !') //expected = [1,2,4]
	console.log(res);
	uc.end();
})
