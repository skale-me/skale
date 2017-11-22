#!/usr/bin/env node

const sc = require('skale').context();

const data = [['hello', 1], ['world', 2], ['cedric', 3], ['cedric', 4]];
const data2 = [['cedric', 3], ['world', 4], ['test', 5]];
const nPartitions = 2;

const a = sc.parallelize(data, nPartitions);
const b = sc.parallelize(data2, nPartitions);

a.coGroup(b).collect(function(err, res) {
  console.log(res);
  console.log(res[0]);
  console.log(res[1]);
  console.log(res[2]);
  console.log(res[3]);  
  sc.end();
});
