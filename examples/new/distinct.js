#!/usr/bin/env node

var uc = require('ugrid').context();

uc.parallelize([ 1, 2, 3, 1, 4, 3, 5 ]).
  distinct().
  collect().toArray(function(err, res) {
	console.log('Success !') //expected [ 4, 1, 5, 2, 3 ]
	console.log(res);
	uc.end();
})
