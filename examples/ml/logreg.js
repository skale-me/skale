#!/usr/bin/env node
'use strict';

var skale = require('skale-engine');
var sizeOf = require('object-sizeof');
var ml = require('../../lib/ml.js');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['f', 'F=ARG', 'SVM input file (default: random data)'],
	['n', 'N=ARG', 'number of observations (default: 842000)'],
	['d', 'D=ARG', 'number of features per observation (default: 16)'],
	['i', 'I=ARG', 'number of iterations (default: 4)'],
	['p', 'P=ARG', 'number of partitons (default: number of workers)']
]).bindHelp().parseSystem();

var file = opt.options.F;
var N = Number(opt.options.N) || 842000;
var D = Number(opt.options.D) || 16;
var nIterations = Number(opt.options.I) ||  4;
var seed = 1;
var P = opt.options.P;
var sample = [1, []];
for (var i = 0; i < D; i++) sample[1].push(Math.random());
var approx_data_size = N * sizeOf(sample);

console.log('Input data: ' + (file || 'random'));
console.log('Observations: ' + N);
console.log('Features per observation: ' + D);
console.log('Partitions: ' + (P || 'number of workers'));
console.log('Iterations: ' + nIterations + '\n');
console.log('Approximate dataset size: ' + Math.ceil(approx_data_size / (1024 * 1024)) + ' Mb');

var sc = skale.context();
var points = file ? sc.textFile(file).map(function (e) {
	var tmp = e.split(' ').map(parseFloat);
	return [tmp.shift(), tmp];
}).persist() : ml.randomSVMData(sc, N, D, seed, P).persist();
var model = new ml.LogisticRegression(sc, points, D, N);

sc.on('connect', function() {console.log('Number of workers: %j', sc.worker.length);});

model.train(nIterations, function() {
	console.log(model.w);
	sc.end();
});
