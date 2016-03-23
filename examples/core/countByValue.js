#!/usr/bin/env node

var sc = require('skale-engine').context();

var data = [[1, 1], [1, 1], [2, 3], [2, 4], [3, 5]];

var nPartitions = 1;

function valueFlatMapper(e) {
	var i, out = [];
	for (i = e; i <= 5; i++) out.push(i);
	return out;
}

sc.parallelize(data, nPartitions)
  .flatMapValues(valueFlatMapper)
  .countByValue()
  .toArray(function(err, res) {
	console.log(res);
	console.assert(JSON.stringify(res) === JSON.stringify([[[1, 1], 2], [[1, 2], 2], [[1, 3], 2], [[1, 4], 2], [[1, 5], 2], [[2, 3], 1], [[2, 4], 2], [[2, 5], 2], [[3, 5], 1]]));
	sc.end();
});
