#!/usr/bin/env node

const sc = require('skale').context();

sc.textFile(process.argv[2]).stream({end: true}).pipe(process.stdout);
