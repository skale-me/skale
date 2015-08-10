#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');
var sizeOf = require('../../utils/sizeof.js');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['f', 'F=ARG', 'Text input file (random if undefined)'],
	['n', 'N=ARG', 'number of observations']
]).bindHelp().parseSystem();

var file = opt.options.F;
// var N = Number(opt.options.N) || 1000000;

// var sample = [1, []];
// for (var i = 0; i < D; i++) sample[1].push(Math.random());
// var approx_data_size = N * sizeOf(sample);

console.log('Input data: ' + (file || 'random'));
// console.log('Number of words: ' + N);
// console.log('Approximate dataset size: ' + Math.ceil(approx_data_size / (1024 * 1024)) + ' Mb\n');

co(function *() {
	var uc = yield ugrid.context();
	var vect  = ['Hello World', 'Hello Cedric'];
	var input = file ? uc.textFile(file) : uc.parallelize(vect);

	console.log(yield input.count());

	// var wordcount = input.flatMap(flatMapper).reduceByKey(reducer, 0);
	// console.log(yield wordcount.count());

	// function flatMapper(line) {
	// 	var tmp = [], data = line.split(' ');
	// 	for (var i = 0; i < data.length; i++) {
	// 		if (data[i].indexOf('/') != -1) continue;	// Skip keys containing '/' character
	// 		tmp.push([data[i], 1]);
	// 	}
	// 	return tmp;		
	// }

	// function reducer(a, b) {return a + b};

	uc.end();

}).catch(ugrid.onError);
