#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.range(10).map(a => a * 2).collect().then(console.log);

sc.range(10, -5, -3).collect().then(console.log);

sc.range(-4, 3).collect(function(err, res) {
  console.log(res);
  sc.end();
});
