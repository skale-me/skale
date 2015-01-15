#!/usr/local/bin/node --harmony

var fs = require('fs');
var ml = require('../lib/ugrid-ml.js');

if (process.argv.length != 4) {
	console.log('Usage: gen_data.js file size_in_Mo')
	process.exit(1);
}

var file = process.argv[2];
var maxSize = process.argv[3] * 1024 * 1024;

var rng = new ml.Random();
var fd = fs.createWriteStream(file);
var fileSize = 0;
var D = 16;

function writeChunk() {
	var line = '';
	for (var i = 0; i < 500; i++) {
		line += 2 * Math.round(Math.abs(rng.randn(1))) - 1;
		line += ' ' + rng.randn(D).join(' ') + '\n';
	}
	var lineSize = Buffer.byteLength(line, 'utf8');
	if ((fileSize + lineSize) > maxSize)
		fd.end();
	else
		fd.write(line, function(err) {fileSize += lineSize; writeChunk();});
}

writeChunk();