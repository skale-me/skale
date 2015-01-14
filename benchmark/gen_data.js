#!/usr/local/bin/node --harmony

var fs = require('fs');
var ml = require('../lib/ugrid-ml.js');

N = 800000 * 4; //observations
D = 16; //features 

var rng = new ml.Random();
//~ var file = '/tmp/data.txt';
var file = '/tmp/data3.txt';

var fd = fs.createWriteStream(file);

for (var i = 0; i < N; i++){
	var line = 2 * Math.round(Math.abs(rng.randn(1))) - 1;
	line += ' ' + rng.randn(D).join(' ') + '\n';
	fd.write(line);
}

fd.end();
