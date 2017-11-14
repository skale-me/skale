#!/usr/bin/env node

var sc = require('skale').context();

sc.parallelize([1, 2, 3, 4]).count()
  .then(function(data) {
    console.log(data);
    console.assert(data === 4);
    sc.end();
  });
