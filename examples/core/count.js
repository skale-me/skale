#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize([1, 2, 3, 4]).count()
  .on('data', function(data) {
	console.log(data);
	console.assert(data == 4);
  })
  .on('end', sc.end);
