#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize([[1,2],[3,4],[3,6]]).
  first().
  then(function(res) {
	console.log(res);
	res.sort();
	console.assert(JSON.stringify(res) === JSON.stringify([[1, 2]]));		  
	sc.end();
});
