#!/usr/bin/env node

var sc = require('skale').context();

sc.parallelize([[1, 2], [3, 4], [3, 6]])
  .lookup(3)
  .then(function(res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([4, 6]));  
    sc.end();
  });
