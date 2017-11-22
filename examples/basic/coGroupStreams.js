#!/usr/bin/env node

const fs = require('fs');
const sc = require('skale').context();

const s1 = sc.lineStream(fs.createReadStream(__dirname + '/kv.data')).map(line => line.split(' '));
const s2 = sc.lineStream(fs.createReadStream(__dirname + '/kv2.data')).map(line =>line.split(' '));

s1.coGroup(s2).collect(function(err, res) {
  console.log(res[0]);
  console.log(res[1]);  
  console.log(res[2]);
  console.log(res[3]);    
  sc.end();
});
