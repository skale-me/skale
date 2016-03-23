#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize([ 1, 2, 3, 1, 4, 3, 5 ]).
  distinct().
  collect().toArray(function(err, res) {
	console.log(res);
	console.assert(JSON.stringify(res) === JSON.stringify([4, 1, 5, 2, 3]));
	sc.end();
})
