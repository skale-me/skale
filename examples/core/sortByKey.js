#!/usr/bin/env node

var sc = require('skale-engine').context();

var data = [['world', 2], ['cedric', 3], ['hello', 1]];
var nPartitions = 2;

sc.parallelize(data, nPartitions)
  .sortByKey()
  .collect()
  .toArray(function(err, res) {
	console.log(res);
	console.assert(JSON.stringify(res) === JSON.stringify([['cedric', 3], ['hello', 1], ['world', 2]]));	
	sc.end();
});
