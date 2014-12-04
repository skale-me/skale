#!/usr/local/bin/node --harmony

var ml = require('../ugrid-ml.js');
var tl = require('./test-lib.js');
var fs = require('fs');

var name = 'filter';
var M = 10;
var a = ml.randn(M);

b = a.filter(tl.positive);

var json = {
	name: name,
	inputs: {a: a},
	output: b
}

fs.writeFile(name + '.json', JSON.stringify(json, null, '\t'));

