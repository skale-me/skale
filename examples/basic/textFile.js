#!/usr/bin/env node

var sc = require('skale').context();

function reducer(a, b) {a.push(b); return a;}
function combiner(a, b) {return a.concat(b);}

var file = __dirname + '/kv.data';

sc.textFile(file)
  .aggregate(reducer, combiner, [], function(err, res) {
    console.log(res);
    res.sort();
    console.assert(JSON.stringify(res) === JSON.stringify(['1 1', '1 1', '2 3', '2 4', '3 5']));  
    sc.end();
  });
