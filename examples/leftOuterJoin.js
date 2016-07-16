#!/usr/bin/env node

var sc = require('skale-engine').context();

var da1 = sc.parallelize([[10, 1], [20, 2]]);
var da2 = sc.parallelize([[10, 'world'], [30, 3]]);

da1.leftOuterJoin(da2)
  .collect(function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([[10, [1, 'world']], [20, [2, null]]]));
    sc.end();
  });
