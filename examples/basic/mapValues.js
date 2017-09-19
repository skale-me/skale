#!/usr/bin/env node

var sc = require('skale-engine').context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

function by2(a) {return a * 2;}

sc.parallelize([['hello', 1], ['world', 2], ['cedric', 3], ['test', 4]])
  .mapValues(by2)
  .aggregate(reducer, combiner, [], function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify([['cedric', 6], ['hello', 2], ['test', 8], ['world', 4]])); 
    sc.end();
  });
