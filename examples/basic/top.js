#!/usr/bin/env node

const sc = require('skale').context();

sc.parallelize([1, 2, 3, 4], 2)
  .top(2).then(function(res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([3, 4]));   
    sc.end();
  });
