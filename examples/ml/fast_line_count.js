#!/usr/local/bin/node --harmony

var fs = require('fs');
var inspect = require('util').inspect;
var Lines = require('../../lib/lines.js');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['f', 'F=ARG', 'Text input file (random if undefined)'],
	['m', 'mode=ARG', '0, 1, 2, 3']	
]).bindHelp().parseSystem();

var file = opt.options.F;
if (file == undefined) process.exit('You must provide an input file')
var mode = Number(opt.options.mode);
var rs = fs.createReadStream(file);
var cnt = 0;

function count(line) {cnt++;}

switch (mode) {
case 0:
	console.log('Linecount using readstream piped to lines transformation')
	var lines = new Lines();
	rs.pipe(lines);
	lines.on('data', count);
	lines.on('end', function() {console.log(cnt)})
	break;
case 1:
	console.log('Linecount using readstream and split method, FASTEST solution')
	var buffer = '';
	rs.on('data', function(chunk) {
		// var lines = (buffer + chunk).split(/\r?\n/g);
		var lines = (buffer + chunk).split(/\r\n|\r|\n/); 		// plus rapide que expression prec
		buffer = lines.pop();
		for (var i = 0; i < lines.length; ++i) {
			// do something with `lines[i]`
			// console.log('found line: ' + inspect(lines[i]));
			count(String(lines[i]));
		}
	});
	rs.on('end', function() {
		// optionally process `buffer` here if you want to treat leftover data without
		// a newline as a "line"
		// console.log('ended on non-empty buffer: ' + inspect(buffer));
		console.log(cnt + 1);
	});
	break;
case 2:
	console.log('Linecount using readstream and no split')
	rs.on('data', function(chunk) {
		for (var i = 0; i < chunk.length; i++)
			if (chunk[i] == 10) count();
	});
	rs.on('end', function() {
		// optionally process `buffer` here if you want to treat leftover data without
		// a newline as a "line"
		// console.log('ended on non-empty buffer: ' + inspect(buffer));
		console.log(cnt + 1);
	});
	break;
case 3:
	console.log('Linecount using readstream, continuous splitting')
	var start = 0, length = 0;
	rs.on('data', function(chunk) {
		var str_chunk = String(chunk);
		for (var i = 0; i < chunk.length; i++) {
			// console.log(typeof(chunk));
			if (chunk[i] == 10) {
				// console.log(String(chunk).substr(start, length))
				count(str_chunk.substr(start, length));
				start = i + 1;
				length = 0;
			} else length++;		
		}
	});
	rs.on('end', function() {
		// optionally process `buffer` here if you want to treat leftover data without
		// a newline as a "line"
		// console.log('ended on non-empty buffer: ' + inspect(buffer));
		console.log(cnt + 1);
	});
	break;	
default:
	console.error('Mode must be 0, 1 or 2');
}
