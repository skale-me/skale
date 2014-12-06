var thunkify = require('thunkify');
var UgridArray = require('./ugrid-array.js');

function rpc(grid, uuid, funStr, input, persist, output, args, resCallback) {
	var payload = {cmd: "task", args: args}
	grid.send_cb('request', {uuid: uuid, payload: payload}, resCallback);
}

module.exports = function UgridContext(grid, worker) {
	this.loadTestData = function(N, D, P) {
		return new UgridArray(grid, worker, [null], 'narrow', 'loadTestData', [N, D, P || 1]);
	}

	this.parallelize = function(localArray, P) {
		return new UgridArray(grid, worker, [null], 'narrow', 'parallelize', [[localArray], P || 1]);
	}

	this.textFile = function(path, P) {
		return new UgridArray(grid, worker, [null], 'narrow', textFile, [path, P || 1]);
	}
};
