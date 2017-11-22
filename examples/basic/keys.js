#!/usr/bin/env node

const sc = require('skale').context();

sc.parallelize([[1, 2], [2, 4], [4, 6]])
  .keys()
  .collect(function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([1, 2, 4]));
    sc.end();
  });
