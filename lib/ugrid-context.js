var thunkify = require('thunkify');
var UgridArray = require('./ugrid-array.js');

module.exports = function UgridContext(grid, worker) {
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

		// var subArray = new Array(worker.length);
		// for (var i = 0; i < worker.length; i++)
		// 	subArray[i] = {};

		// for (var i = 0; i < localArray.length; i++)
		// 	subArray[i % worker.length][i] = localArray[i];

		// console.log(subArray);

		return new UgridArray(grid, worker, [], 'narrow', 'parallelize', [subArray, P || 1]);

	}

	this.textFile = function(path, P) {
		return new UgridArray(grid, worker, [], 'narrow', textFile, [path, P || 1]);
	}
};
