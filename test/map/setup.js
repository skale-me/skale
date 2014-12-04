#!/usr/local/bin/node --harmony

var fs = require('fs');
var ml = require('../../lib/ugrid-ml.js');
var tl = require('../test-lib.js');

var name = 'map';
var M = 5;  // taille du vecteur a
var a = ml.randn(M);

b = a.map(tl.doubles);

var json = {
	name: name,
	input: a,
	output: b
}

fs.writeFile(name + '.json', JSON.stringify(json, null, '\t'));
