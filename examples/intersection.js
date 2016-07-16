#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9]).
  intersection(sc.parallelize([5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])).
  collect(function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([5, 6, 7, 8, 9]));    
    sc.end();
  });
