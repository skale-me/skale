#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.gzipFile(__dirname + '/xxx.gz').count().then(function (res) {console.log(res); sc.end();});
