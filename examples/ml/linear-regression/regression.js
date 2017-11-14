#!/usr/bin/env node

'use strict';

(async function main() {

  const sc = require('skale').context();
  const ml = require('skale/ml');

  const labelFeatures = sc.textFile(__dirname + '/sample_linear_regression_data.txt')
    .map(a => {
      const b = a.split(' ');
      return [Number(b.shift()), b.map(a => Number(a.split(':').pop()))];
    });
  //console.log(await labelFeatures.take(1));

  const model = ml.SGDLinearModel({});
  await model.fit(labelFeatures, 10);
  console.log('model:', model);

  sc.end();

})(); // main
