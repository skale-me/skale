#!/usr/bin/env node

var sc = require('skale-engine').context();

var file = __dirname + '/kv.data';

sc.textFile(file)
  .take(1)
  .then(function (res) {
    console.log(res);
    sc.end();
  });
