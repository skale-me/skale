#!/usr/bin/env node

var fs = require('fs');

if (process.argv.length != 5) {
	console.log('Usage: gen_data.js file D size_in_Mo');
	process.exit(1);
}

var file = process.argv[2];
var D = process.argv[3];
var maxSize = process.argv[4] * 1024 * 1024;

var rng = new Random();
var fd = fs.createWriteStream(file);
var fileSize = 0;

function writeChunk() {
	var line = '';
	for (var i = 0; i < 500; i++) {
		line += 2 * Math.round(Math.abs(rng.randn(1))) - 1;
		line += ' ' + rng.randn(D).join(' ') + '\n';
	}
	var lineSize = Buffer.byteLength(line, 'utf8');
	if ((fileSize + lineSize) > maxSize) fd.end();
	else fd.write(line, function() {fileSize += lineSize; writeChunk();});
}

writeChunk();

function Random(initSeed) {
	this.seed = initSeed || 1;

	this.next = function () {
	    var x = Math.sin(this.seed++) * 10000;
	    return (x - Math.floor(x)) * 2 - 1;
	};

	this.reset = function () {
		this.seed = initSeed;
	};

	this.randn = function (N) {
		var w = new Array(N);
		for (var i = 0; i < N; i++)
			w[i] = this.next();
		return w;
	};
}
