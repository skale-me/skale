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
	// task: function(msg) {
	// 	vm.runInThisContext('var Task = ' + msg.data.args.lastStr);
	// 	task = new Task();

	// 	var res = task.stage[task.stageIdx++](ml, STAGE_RAM, RAM, msg.data.args.lineages, msg.data.args.action);

	// 	if (task.stageIdx == task.stage.length) {
	// 		grid.send_cb('answer', {uuid: msg.from, cmd_id: msg.cmd_id, payload: {result: res || null}}, function(err2, res2) {
	// 			if (err2) throw err2;
	// 		});
	// 	} else {
	// 		function rpc(grid, uuid, args, callback) {
	// 			grid.send_cb('request', {
	// 				uuid: uuid, payload: {cmd: "shuffle", args: args}
	// 			}, callback);
	// 		}

	// 		// Map partitions to workers to be (need to use hashcoding)
	// 		// Ok if partition name is a Number for now
	// 		var map = task.worker.map(function(n) {return []});
	// 		for (var i in res) {
	// 			// Do the hashcoding here
	// 			var idx = i % task.worker.length;
	// 			map[idx].push(res[i])
	// 		}

	// 		// Find my partition and process it using task specific stage output callback
	// 		for (var i = 0; i < map.length; i++)
	// 			if (task.worker[i] == myId) {
	// 				console.log('Found myself')
	// 				STAGE_RAM = map[i];
	// 				if (true) {	// must the next stage be triggered ?
	// 					res = task.stage[task.stageIdx++](ml, STAGE_RAM, RAM, msg.data.args.lineages, msg.data.args.action);
	// 					// answer send must be handled directly at the end of last stage (NOT HERE)
	// 					grid.send_cb('answer', {uuid: msg.from, cmd_id: msg.cmd_id, payload: {result: res || null}}, function(err2, res2) {
	// 						if (err2) throw err2;
	// 					});						
	// 				}
	// 			}

	// 		// Send partitions to other workers
	// 		for (var i = 0; i < map.length; i++)
	// 			if (task.worker[i] != myId)
	// 				rpc(grid, task.worker[i], map[i], function(err, res) {if (err) throw err;});

	// 		// console.log('\nlazy-worker.js task rpc: hashcode stage result, dispatch partitions to workers')
	// 		// console.log(res);

	// 		// for (var i in task.worker)
	// 		// 	rpc(grid, task.worker[i], res, function(err, res) {
	// 		// 		if (err) throw err;
	// 		// 	})
	// 	}
	// },
	task: function(msg) {
		vm.runInThisContext('var Task = ' + msg.data.args.lastStr);
		task = new Task(grid, ml, STAGE_RAM, RAM, msg.data.args.lineages, msg.data.args.action, function(res) {
			grid.send_cb('answer', {uuid: msg.from, cmd_id: msg.cmd_id, payload: {result: res || null}}, function(err2, res2) {
				if (err2) throw err2;
			});			
		});
		task.run();
	},	
	shuffle: function(msg) {
		console.log('\nlazy-worker.js shuffle rpc: call task stage specific processing and run next stage')
		console.log(msg)
	}
};

grid.connect_cb(function(err, res) {
	console.log("uuid: " + res.uuid);
	grid.uuid = res.uuid;
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
