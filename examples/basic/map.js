#!/usr/bin/env node

var sc = require('skale').context();

function by2(a, args) {return a * 2 * args.bias;}
function sum(a, b) {return a + b;}

sc.parallelize([1, 2, 3, 4])
  .map(by2, {bias: 2})
  .reduce(sum, 0, function(err, res) {
    console.log(res);
    console.assert(res === 40);
    sc.end();
  });
