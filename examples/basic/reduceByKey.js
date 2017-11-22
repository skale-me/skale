#!/usr/bin/env node

const sc = require('skale').context();

const data = [['hello', 1], ['hello', 1], ['world', 1]];
const nPartitions = 2;

const init = 0;

function reducer(a, b) {return a + b;}

sc.parallelize(data, nPartitions)
  .reduceByKey(reducer, init)
  .collect(function(err, res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([['hello', 2], ['world', 1]]));   
    sc.end();
  });
