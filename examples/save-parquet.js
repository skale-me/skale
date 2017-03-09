#!/usr/bin/env node

var sc = require('skale-engine').context();

var schema = {
  int1: {type: 'int32'},
  int2: {type: 'int32'},
};

sc.range(900).
  map(a => [a, 2 * a]).
  save('/tmp/truc', {parquet: {schema: schema}}, (err, res) => {
    console.log(res);
    sc.end();
  });
