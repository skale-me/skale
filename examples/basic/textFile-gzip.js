#!/usr/bin/env node

var sc = require('skale').context();

sc.textFile(__dirname + '/xxx.gz').count().then(function (res) {console.log(res); sc.end();});
