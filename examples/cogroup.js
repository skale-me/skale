#!/usr/bin/env node
'use strict';

var uc = new require('ugrid').Context();

var file = __dirname + '/../test/support/kv.data';

var a = uc.textFile(file).map(function(line) {return line.split(' ').map(Number)});
var b = uc.textFile(file).map(function(line) {return line.split(' ').map(Number)});

a.coGroup(b).collect().toArray(function(err, res) {
	console.log(res);
	console.log(res.length);
	uc.end();
})
