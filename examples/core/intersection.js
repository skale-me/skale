#!/usr/bin/env node

var sc = require('skale').context();

sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9]).
  intersection(sc.parallelize([5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])).
  collect().
  toArray(function(err, res) {
	console.log('Success !')  //expected = [5,6,7,8,9]
	console.log(res);
	sc.end();
})
