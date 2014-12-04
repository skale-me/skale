#!/usr/local/bin/node --harmony

// Example sans workers

var ml = require('../ugrid-ml.js');
var tl = require('./test-lib.js');

var M = 5;  // taille du vecteur a
var a = [];

//init vecteur a
for (var i = 0; i < M; i++)
	a[i] = i;

console.log('a =');
console.log(a);
console.log('\n');

result = a.map(tl.doubles);
console.log(result);
