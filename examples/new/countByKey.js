#!/usr/bin/env node

var uc = require('ugrid').context();

uc.parallelize([[1,2],[3,4],[3,6]]).
  countByKey().
  toArray(function(err, res) {
	console.log('Success !') //expected expected = [[3,2],[1,1]]
	console.log(res);
	uc.end();
})
