#!/usr/bin/env node

const sc = require('skale').context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function dup(a) {return [a, a];}

sc.parallelize([1, 2, 3, 4])
  .flatMap(dup)
  .aggregate(reducer, combiner, [], function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([1, 1, 2, 2, 3, 3, 4, 4])); 
    sc.end();
  });
