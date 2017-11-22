#!/usr/bin/env node

const sc = require('skale').context();

sc.parallelize([1, 2, 3, 4], 2)
  .take(2).then(function(res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([1, 2]));   
    sc.end();
  });
