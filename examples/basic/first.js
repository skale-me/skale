#!/usr/bin/env node

const sc = require('skale').context();

sc.parallelize([[1,2],[3,4],[3,6]]).
  first().
  then(function(res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([1, 2]));
    sc.end();
  });
