#!/usr/bin/env node

var sc = require('skale-engine').context();

var d1 = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];
var d2 = [[1, 1], [1, 1], [2, 3]];

sc.parallelize(d1)
  .subtract(sc.parallelize(d2))
  .collect(function(err, res) {
	console.log(res);
	res.sort();
	console.assert(JSON.stringify(res) === JSON.stringify([[2, 4], [3, 5]])); 	
	sc.end();
});
