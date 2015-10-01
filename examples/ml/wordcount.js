#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['f', 'F=ARG', 'Text input file (random if undefined)']
]).bindHelp().parseSystem();

var file = opt.options.F;
var minLength = 25;
var maxLength = 25;

console.log('Input data: ' + (file || 'random'));

co(function *() {
	var uc = yield ugrid.context();
	var vect  = ['Hello World', 'Hello Cedric'];
	var input = file ? uc.textFile(file) : uc.parallelize(vect);

	console.time('duration');
	var wordcount = input.flatMap(flatMapper).reduceByKey(reducer, 0);
	console.log(yield wordcount.count());
	console.timeEnd('duration');

	function flatMapper(line) {
		var tmp = [], data = line.split(' ');
		for (var i = 0; i < data.length; i++) tmp.push([data[i], 1]);
		return tmp;
	}

	function reducer(a, b) {return a + b};

	uc.end();
}).catch(ugrid.onError);