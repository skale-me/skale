#!/usr/bin/env node

const sc = require('skale').context();

const data = [['hello', 1], ['hello', 1], ['world', 1]];
const nPartitions = 1;

const a = sc.parallelize(data, nPartitions).groupByKey().persist();

a.collect(function(err, res) {
  console.log(res);
  console.log('First ok!');
  a.collect(function(err, res) {
    console.log(res);
    console.log('Second ok !');
    sc.end();
  });
});
