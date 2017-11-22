#!/usr/bin/env node

const sc = require('skale').context();

const data = [4, 6, 10, 5, 1, 2, 9, 7, 3, 0];
const nPartitions = 3;

function keyFunc(data) {return data;}

sc.parallelize(data, nPartitions)
  .sortBy(keyFunc)
  .collect(function(err, res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([0, 1, 2, 3, 4, 5, 6, 7, 9, 10]));  
    sc.end();
  });
