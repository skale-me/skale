#!/usr/bin/env node

var sc = require('skale-engine').context();
var HashPartitioner = require('skale').HashPartitioner;

var data = [['hello', 1], ['world', 1], ['hello', 2], ['world', 2], ['cedric', 3]];

sc.parallelize(data)
  .partitionBy(new HashPartitioner(3))
  .collect()
  .toArray(function(err, res) {
	console.log(res);
	console.assert(JSON.stringify(res) === JSON.stringify([['world', 1], ['world', 2],['hello', 1],['hello', 2],['cedric', 3]]));	
	sc.end();
});
