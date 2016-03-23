#!/usr/bin/env node

var sc = require('skale-engine').context();

var file = process.argv[2] || '/etc/hosts';

sc.textFile(file)
  .flatMap(line => line.split(' '))
  .map(word => [word, 1])
  .reduceByKey((a, b) => a + b, 0)
  .count().then(console.log)
