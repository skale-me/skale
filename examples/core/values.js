#!/usr/bin/env node

var sc = require('skale').context();

sc.parallelize([[1,2],[2,4],[4,6]]).
  values().
  collect().
  toArray(function(err, res) {
	console.log('Success !') //expected = [2,4,6]
	console.log(res);
	sc.end();
})
