#!/usr/bin/env node

var sc = require('skale-engine').context();

var da1 = sc.parallelize([[10, 1], [20, 2]]);
var da2 = sc.parallelize([[10, 'world'], [30, 3]]);

da1.leftOuterJoin(da2)
   .collect()
   .toArray(function(err, res) {
	console.log(res);
	console.assert(JSON.stringify(res) === JSON.stringify([[20, [2, null]], [10, [1, 'world']]])); 
	sc.end();
});
