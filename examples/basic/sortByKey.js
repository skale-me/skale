#!/usr/bin/env node

const sc = require('skale').context();

const data = [['world', 2], ['cedric', 3], ['hello', 1]];
const nPartitions = 2;

sc.parallelize(data, nPartitions)
  .sortByKey()
  .collect(function(err, res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([['cedric', 3], ['hello', 1], ['world', 2]]));  
    sc.end();
  });
