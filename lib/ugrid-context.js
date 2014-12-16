'use strict';

var thunkify = require('thunkify');
var UgridArray = require('./ugrid-array.js');

module.exports = function UgridContext(grid, worker) {
	this.randomSVMData = function(N, D, seed, P) {
		var rem = N % worker.length;
		var L = (N - rem) / worker.length;
		var Ni = [], seeds = [], acc;

		// Change the distribution order here
		// better loop over N and fill each worker
		// sequentially
		for (var i = 0; i < worker.length; i++)
			Ni.push(L);
		Ni[0] += rem;

		seeds[0] = seed || 1;
		acc = seeds[0] + (L + rem) * (D + 1);
		for (var i = 1; i < worker.length; i++) {
			seeds[i] = acc;
			acc += L * (D + 1);
		}

		return new UgridArray(grid, worker, [], 'narrow', 'randomSVMData', [Ni, D, seeds, P || 1]);
	}

	this.loadTestData = function(N, D, P) {
		return new UgridArray(grid, worker, [], 'narrow', 'loadTestData', [N, D, P || 1]);
	}

	this.parallelize = function(localArray, P) {
		// Split localArray in subArrays
		var subArray = new Array(worker.length);
		for (var i = 0; i < worker.length; i++)
			subArray[i] = [];
		for (var i = 0; i < localArray.length; i++)
			subArray[i % worker.length].push(localArray[i]);

		return new UgridArray(grid, worker, [], 'narrow', 'parallelize', [subArray, P || 1]);
	}

	this.textFile = function(path, P) {
		var mod = worker.length;
		var base = [];
		for (var i = 0; i < mod; i++) 
			base[i] = i;
		return new UgridArray(grid, worker, [], 'narrow', 'textFile', [path, base, mod, P || 1]);
	}
};
