#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.env.MY_VAR = 'Hello';

sc.range(5).
  map(function (i, obj, wc) {
   return process.env.MY_VAR + i;
  }).
  collect(function (err, res) {
    console.log(res);
    sc.end();
  });
