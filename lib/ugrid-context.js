var thunkify = require('thunkify');
var UgridArray = require('./ugrid-array.js');

function rpc(grid, uuid, funStr, input, persist, output, args, resCallback) {
	var payload = {cmd: "task", args: args}
	grid.send_cb('request', {uuid: uuid, payload: payload}, resCallback);
}

module.exports = function UgridContext(grid, worker) {
	this.loadTestData = function(N, D, P) {
		var array = new UgridArray(grid, worker);
		var srcGridArrayId = null;
		var transform = 'loadTestData';
		var args = [N, D, P || 1];
		array.setDependency([srcGridArrayId], transform, args);
		return array;
	}

	this.parallelize = function(localArray, P) {
		var array = new UgridArray(grid, worker);
		var srcGridArrayId = null;
		var transform = 'parallelize';
		var args = [[localArray], P || 1];
		array.setDependency([srcGridArrayId], transform, args);
		return array;
	}

	this.textFile = function(path, P) {
		var array = new UgridArray(grid, worker);
		var srcGridArrayId = null;
		var transform = 'textFile';
		var args = [path, P || 1];
		array.setDependency([srcGridArrayId], transform, args);
		return array;	
	}
};
