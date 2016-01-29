#!/usr/bin/env node

var sc = require('skale').context();

sc.range(10).map(a => a * 2).collect().toArray().then(console.log);

sc.range(10, -5, -3).collect().toArray().then(console.log);

sc.range(-4, 3).collect().toArray(function(err, res) {
	console.log(res);
	sc.end();
})
