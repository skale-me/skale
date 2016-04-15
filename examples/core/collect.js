#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize([1, 2, 3, 4], 2).collect().toArray(function(err, res) {
	console.assert(JSON.stringify(res) === JSON.stringify([1, 2, 3, 4]));
	console.log(res);
	sc.end();
});
