#!/usr/local/bin/node --harmony

var ml = require('../ugrid-ml.js');
var fs = require('fs');

var name = 'union';
var M = process.argv[2] || 2;
var N = process.argv[3] || 4;

a = ml.randn(M);
b = ml.randn(N);
c = a.concat(b);  // union de A et B

var json = {
	name: name,
	inputs: {a: a, b: b},
	output: c
}

fs.writeFile(name + '.json', JSON.stringify(json, null, '\t'));
