#!/usr/bin/env node

var uc = require('ugrid').context();

uc.parallelize([[1,2],[3,4],[3,6]]).
  lookup(3).
  toArray(function(err, res) {
	console.log('Success !') //expected [ 4, 6 ]
	console.log(res);
	uc.end();
})
