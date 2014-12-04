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

	// this.lazyTextFile = function(path) {
	// 	var array = new UgridArray(grid, worker);
	// 	var args = [path /*, worker.length, workerIdx*/];
	// 	array.setDependency(null, 'lazyTextFileRPC', args);
	// 	return array;
	// }

	// // Non lazy, need to be removed
	// this.textFile_cb = function(path, callback) {
	// 	var array = new UgridArray(grid, worker), nAnswer = 0;

	// 	for (var i in worker)
	// 		rpc(grid, worker[i], 'textFile', null, null, array.id, [path, worker.length, i], function(err, res) {
	// 			if (++nAnswer == worker.length)
	// 				callback(null, array);
	// 		})
	// }

	// Wrapper for co-routine lib
	// this.textFile = thunkify(this.textFile_cb);	
};
