#!/usr/bin/env node

var fs = require('fs');
var uc = new require('ugrid').Context();

var stream = fs.createReadStream('/Users/cedricartigue/work/ugrid/examples/new/kv.data');

// var stream = process.stdin;
uc.lineStream(stream).collect().toArray(function(err, res) {
	console.log('Success !')
	console.log(res);
	uc.end();
});
