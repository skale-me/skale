#!/usr/local/bin/node --harmony
'use strict';

var co = require('co');
var ugrid = require('../..');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['f', 'F=ARG', 'SVM input file (random data if undefined)'],
	['k', 'K=ARG', 'number of means'],
	['n', 'N=ARG', 'number of observations'],
	['d', 'D=ARG', 'number of features per observation'],
	['i', 'I=ARG', 'number of iterations']
]).bindHelp().parseSystem();

var file = opt.options.F;
var N = Number(opt.options.N) || 842000;
var K = Number(opt.options.K) || 10;
var D = Number(opt.options.D) || 16;
var nIterations = Number(opt.options.I) || 4;
var seed = 1;

console.log('Input data: ' + (file || 'random'));
console.log('Number of observations: ' + N);
console.log('Number of clusters: ' + K);
console.log('Features per observation: ' + D);
console.log('Iterations: ' + nIterations + '\n');

co(function *() {
	var uc = yield ugrid.context();
	var points = file ? uc.textFile(file).map(function (e) {
		var tmp = e.split(' ').map(parseFloat);
		tmp.shift();
		return tmp;
	}).persist() : uc.randomSVMData(N, D, seed).map(function(e) {
		return e[1]}
	).persist();
	var model = new ugrid.ml.KMeans(points, K);
	yield model.train(nIterations);
	// console.log(model.means)
	uc.end();
}).catch(ugrid.onError);
