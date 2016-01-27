#!/usr/bin/env node

var fs = require('fs');
var sc = new require('skale').Context();

var s1 = sc.lineStream(fs.createReadStream(__dirname + '/kv.data')).map(function(line) {return line.split(' ')});
var s2 = sc.lineStream(fs.createReadStream(__dirname + '/kv2.data')).map(function(line) {return line.split(' ')});

s1.coGroup(s2).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res[0]);
	console.log(res[1]);	
	console.log(res[2]);
	console.log(res[3]);		
	sc.end();
});
