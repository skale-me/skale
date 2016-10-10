#!/usr/bin/env node

var sc = require('skale-engine').context();
var input = sc.textFile('s3://skale-demo/datasets/restaurants-ny.json.gz');
//var input = sc.textFile('s3://skale-demo/datasets/restaurants-ny.json');
//var s = input.stream();
//s.pipe(process.stdout);
//s.on('end', sc.end);

input.count(function (err, res) {
  console.log(res);
  sc.end();
});
