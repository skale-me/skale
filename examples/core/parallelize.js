#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize([1, 2, 3, 4, 5])
  .collect(function(err, res) {
	console.log(res);
	console.assert(JSON.stringify(res) === JSON.stringify([1, 2, 3, 4, 5]));
	sc.end();
});
