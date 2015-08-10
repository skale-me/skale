#!/usr/local/bin/node --harmony

var fs = require('fs');

if (process.argv.length != 4) {
	console.log('Usage: gen_text.js file size_in_Mo');
	process.exit(1);
}

var file = process.argv[2];
var maxSize = process.argv[3] * 1024 * 1024;

var fd = fs.createWriteStream(file);
var fileSize = 0;

function writeChunk() {
	var line = 'hello world and cedric and marc and idoia and renaud\n';
	var lineSize = Buffer.byteLength(line, 'utf8');
	if ((fileSize + lineSize) > maxSize) fd.end();
	else fd.write(line, function() {fileSize += lineSize; writeChunk();});
}

writeChunk();
