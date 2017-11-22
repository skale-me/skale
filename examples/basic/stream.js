#!/usr/bin/env node

const sc = require('skale').context();
// const s = sc.range(20).stream({gzip: true});
//const s = sc.range(20).stream();
const s = sc.range(20).stream({end: true});
s.pipe(process.stdout);
//s.on('end', sc.end);
