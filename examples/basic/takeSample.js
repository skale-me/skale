#!/usr/bin/env node

process.env.SKALE_RANDOM_SEED = "skale";

const sc = require('skale-engine').context();

sc.range(100)
  .takeSample(false, 4, function(err, res) {
    console.log(res);
    sc.end();
  });
