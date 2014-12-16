'use strict';

var thunkify = require('thunkify');
var UgridClient = require('./ugrid-client.js');
var UgridArray = require('./ugrid-array.js');

module.exports = UgridContext;

function UgridContext(arg) {
	if (!(this instanceof UgridContext))
		return new UgridContext(arg);

	var grid = new UgridClient({host: arg.host, port: arg.port, data: {type: 'master'}});
	var worker;

	this.init_cb = function (callback) {
		grid.connect_cb(function () {
			grid.devices_cb({type: 'worker'}, function(err, res) {
				worker = res;
				callback();
			});
		});
	};

	this.end = function () {
		grid.disconnect();
	};

	this.randomSVMData = function (N, D, seed, P) {

		// N doit etre le nombre de vecteurs par partition
		// et non pas par worker

		var rem = N % worker.length;
		var L = (N - rem) / worker.length;
		var Ni = [], seeds = [], acc;

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
	};

	this.loadTestData = function (N, D, P) {
		return new UgridArray(grid, worker, [], 'narrow', 'loadTestData', [N, D, P || 1]);
	};

	this.parallelize = function (localArray, P) {
		// Split localArray in subArrays
		var subArray = new Array(worker.length);
		for (var i = 0; i < worker.length; i++)
			subArray[i] = [];
		for (var i = 0; i < localArray.length; i++)
			subArray[i % worker.length].push(localArray[i]);

		return new UgridArray(grid, worker, [], 'narrow', 'parallelize', [subArray, P || 1]);
	};

	this.textFile = function (path, P) {
		return new UgridArray(grid, worker, [], 'narrow', textFile, [path, P || 1]);
	};

	this.init = thunkify(this.init_cb);
};
