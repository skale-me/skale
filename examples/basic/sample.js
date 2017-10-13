#!/usr/bin/env node

process.env.SKALE_RANDOM_SEED = 'skale';

var sc = require('skale-engine').context();

sc.range(100)
  .sample(false, 0.1)
  .collect(function(err, res) {
    console.log(res);
    sc.end();
  });
