#!/usr/bin/env node

var sc = require('skale').context();

var data = [
  ['hello', [12, 'param1', 'param2']],
  ['hello', [10, 'param3', 'param4']],
  ['world', [5, 'param5', 'param6']]
];
var nPartitions = 1;

var init = [0, []];

function reducer(a, b) {
  a[0] += b[0];
  a[1].push([b[1], b[2]]);
  return a;
}

sc.parallelize(data, nPartitions)
  .reduceByKey(reducer, init)
  .collect(function(err, res) {
    console.log(res[0][0], res[0][1]);
    sc.end();
  });
