#!/usr/bin/env node

const sc = require('skale').context();

sc.textFile(__dirname + '/xxx.gz').count().then(function (res) {console.log(res); sc.end();});
