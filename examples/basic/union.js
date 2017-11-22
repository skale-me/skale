#!/usr/bin/env node

const sc = require('skale').context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

const a = sc.parallelize([1, 2, 3, 4]);
const b = sc.parallelize([5, 6, 7, 8]);

a.union(b).aggregate(reducer, combiner, [], function(err, res) {
  console.log(res);
  res.sort();
  console.assert(JSON.stringify(res) === JSON.stringify([1, 2, 3, 4, 5, 6, 7, 8])); 
  sc.end();
});
