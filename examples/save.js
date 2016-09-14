#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.range(900).save('/tmp/truc', {gzip: true}, (err, res) => {
// sc.range(900).save('s3://skale-demo/test/s1', {gzip: true}, (err, res) => {
  console.log(res);
  sc.end();
});
