#!/usr/local/bin/node --harmony

var co = require('co');
var fs = require('fs');
var ugrid = require('../..');

var data_len = process.argv[2] || 1;
var block_len = process.argv[3] || 1;

console.log("data_len = " + data_len);
console.log("block_len = " + block_len);

co(function *() {
	var uc = yield ugrid.context();

	var input = fs.createWriteStream('/tmp/custom_stream');
	var output = fs.createReadStream('/tmp/custom_stream');

	for (var i = 0; i < data_len; i++) {
		var o = {i : i};
		input.write(JSON.stringify(o) + "\n");
	}

	var data = uc.stream(output, {N: block_len}).collect({stream: true});

	data.on('data', function(res) {
		console.dir(res);
	});

	data.on('end', function(res) {
		uc.end();
	});


}).catch(ugrid.onError);



