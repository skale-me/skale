#!/usr/bin/env node

const sc = require('skale').context();
const input = sc.textFile('s3://skale-demo/datasets/*-ny.json.gz');
//const input = sc.textFile('s3://skale-demo/datasets/restaurants-ny.json.gz');
//const input = sc.textFile('s3://skale-demo/datasets/restaurants-ny.json');
//const s = input.stream();
//s.pipe(process.stdout);
//s.on('end', sc.end);

input.count(function (err, res) {
  console.log(res);
  sc.end();
});
