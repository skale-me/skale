#!/usr/bin/env node

const sc = require('skale').context();

const data = [['hello', 1], ['world', 2], ['world', 3]];
const data2 = [['cedric', 3], ['world', 4]];
const nPartitions = 4;

const a = sc.parallelize(data, nPartitions);
const b = sc.parallelize(data2, nPartitions);

a.join(b).collect(function(err, res) {
  console.log(res);
  console.assert(JSON.stringify(res) === JSON.stringify([['world', [2, 4]],['world',[3, 4]]])); 
  sc.end();
});
