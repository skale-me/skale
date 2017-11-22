#!/usr/bin/env node

const sc = require('skale').context();

const data = [1, 2, 3, 4, 5, 6];
const data2 = [7, 8, 9, 10, 11, 12];
const nPartitions = 3;

const a = sc.parallelize(data, nPartitions);
const b = sc.parallelize(data2, nPartitions);

a.cartesian(b)
  .collect(function(err, res) {
    res.sort();
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify([
      [1, 10], [1, 11], [1, 12], [1, 7], [1, 8], [1, 9],
      [2, 10], [2, 11], [2, 12], [2, 7], [2, 8], [2, 9],
      [3, 10], [3, 11], [3, 12], [3, 7], [3, 8], [3, 9],
      [4, 10], [4, 11], [4, 12], [4, 7], [4, 8], [4, 9],
      [5, 10], [5, 11], [5, 12], [5, 7], [5, 8], [5, 9],
      [6, 10], [6, 11], [6, 12], [6, 7], [6, 8], [6, 9]
    ]));
    sc.end();
  });
