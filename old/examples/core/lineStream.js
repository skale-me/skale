#!/usr/bin/env node

var fs = require('fs');
var assert = require('assert');
var uc = new require('ugrid').Context();

var stream = fs.createReadStream(__dirname + '/kv.data');

uc.lineStream(stream).collect().toArray(function(err, res) {
	assert(JSON.stringify(res) === JSON.stringify(['1 1', '1 1', '2 3', '2 4', '3 5']));	
	console.log('Success !')
	console.log(res);
	uc.end();
});
