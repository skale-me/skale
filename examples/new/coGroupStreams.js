#!/usr/bin/env node

var fs = require('fs');
var uc = new require('ugrid').Context();

var s1 = uc.lineStream(fs.createReadStream('examples/new/kv.data')).map(function(line) {return line.split(' ')});
var s2 = uc.lineStream(fs.createReadStream('examples/new/kv2.data')).map(function(line) {return line.split(' ')});

s1.coGroup(s2).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res[0]);
	console.log(res[1]);	
	console.log(res[2]);
	console.log(res[3]);		
	uc.end();
});
