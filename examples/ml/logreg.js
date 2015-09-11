#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');
var sizeOf = require('../../utils/sizeof.js');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['f', 'F=ARG', 'SVM input file (random data if undefined)'],
	['n', 'N=ARG', 'number of observations'],
	['d', 'D=ARG', 'number of features per observation'],
	['i', 'I=ARG', 'number of iterations']
]).bindHelp().parseSystem();

var file = opt.options.F;
var N = Number(opt.options.N) || 842000;
var D = Number(opt.options.D) || 16;
var nIterations = Number(opt.options.I) ||  4;
var seed = 1;
var P = 100;						// increasing improve performances
var sample = [1, []];
for (var i = 0; i < D; i++) sample[1].push(Math.random());
var approx_data_size = N * sizeOf(sample);

console.log('Input data: ' + (file || 'random'));
console.log('Number of observations: ' + N);
console.log('Features per observation: ' + D);
console.log('Iterations: ' + nIterations + '\n');
console.log('Approximate dataset size: ' + Math.ceil(approx_data_size / (1024 * 1024)) + ' Mb');

co(function *() {
	var uc = yield ugrid.context();
	var points = file ? uc.textFile(file).map(function (e) {
		var tmp = e.split(' ').map(parseFloat);
		return [tmp.shift(), tmp];
	}).persist() : uc.randomSVMData(N, D, seed, P).persist();
	var model = new ugrid.ml.LogisticRegression(points, D, N);
	yield model.train(nIterations);
	console.log(model.w);
	uc.end();
}).catch(ugrid.onError);
