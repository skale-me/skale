#!/usr/bin/env node

var sc = require('skale').context();

sc.textFile(process.argv[2]).stream({end: true}).pipe(process.stdout);
