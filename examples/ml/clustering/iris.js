#!/usr/bin/env node

'use strict';

(async function main() {

  const sc = require('skale-engine').context();
  const ml = require('skale-engine/ml');

  const rawdata = sc.textFile(__dirname + '/iris.csv');
  console.log(await rawdata.collect());

  const data = rawdata
    .filter(a => a[0] !== 'S')
    .map(a => a.split(','))
    .map(a => [a.pop(), a.map(Number)])   // [species, array of numeric features]
    .persist();
  //console.log(await data.collect());

  const model = new ml.KMeans(3);
  await model.fit(data.map(a => a[1]));
  console.log(model);

  const predicted = data.map((a, model) => [a[0], model.predict(a[1])], model);
  console.log(await predicted.collect());
  sc.end();

})(); // main
