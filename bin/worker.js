#!/usr/local/bin/node

var vm = require('vm');
var UgridClient = require('../lib/ugrid-client.js');
var ml = require('../lib/ugrid-ml.js')

var grid = new UgridClient({
	host: process.argv[2] || 'localhost',
	port: process.argv[3] || 12346,
	data: {type: 'worker'}
});

var RAM = {}, STAGE_RAM = {}, task;

var worker_request = {
	task: function(msg) {
		vm.runInThisContext('var Task = ' + msg.data.args.lastStr);
		task = new Task();

		var res = task.run[task.stageIdx++](ml, STAGE_RAM, RAM, msg.data.args.lineages, msg.data.args.action);

		if (task.stageIdx == task.run.length) {
			grid.send_cb('answer', {uuid: msg.from, cmd_id: msg.cmd_id, payload: {result: res || null}}, function(err2, res2) {
				if (err2) throw err2;
			});
		} else {
			function rpc(grid, uuid, args, callback) {
				grid.send_cb('request', {
					uuid: uuid, payload: {cmd: "shuffle", args: args}
				}, callback);
			}

			console.log('\nlazy-worker.js task rpc: hashcode stage result, dispatch partitions to workers')
			console.log(res);
			
			for (var i in task.worker)
				rpc(grid, task.worker[i], res, function(err, res) {
					if (err) throw err;
				})
		}
	},
	shuffle: function(msg) {
		console.log('\nlazy-worker.js shuffle rpc: call task stage specific processing and run next stage')
		console.log(msg)
	}
};

grid.connect_cb(function(err, res) {
	console.log("uuid: " + res.uuid);
	grid.on('request', function(msg) {
		try {worker_request[msg.data.cmd](msg);} 
		catch (error) {
			console.log(msg.data.fun + ' error : ' + error);
			grid.send_cb('answer', {uuid: msg.from, cmd_id: msg.cmd_id, payload: {err: error}}, function(err2, res2) {
				if (err2) throw err2;
			});
		}
	});
});
