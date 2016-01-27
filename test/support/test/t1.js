#!/usr/bin/env node

if (process.argv[2] == 'local') {
	var skale = require('../local.js');
	console.log('loc')
} else {
	var skale = require('skale');
	console.log('noloc')
}
var v = [
	[[4, 1], [1, 1], [2, 3], [2, 4], [3, 5]],
	[[0, 5], [1, 6], [2, 7], [3, 9], [0, 9]],
];

var sc = skale.context();
var a, b, s;

a = sc.parallelize(v[0]);
b = sc.parallelize(v[1]);

s = a.join(b).collect();

s.on('data', console.log)
s.on('end', sc.end)
