#!/usr/bin/env node

const fs = require('fs');
const sc = require('skale').context();

const stream = fs.createReadStream(__dirname + '/kv.data');

sc.lineStream(stream)
  .collect(function(err, res) {
    console.log(res);
    console.assert(JSON.stringify(res) === JSON.stringify(['1 1', '1 1', '2 3', '2 4', '3 5']));  
    sc.end();
  });
