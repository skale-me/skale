#!/usr/local/bin/node

var cluster = require('cluster');
var vm = require('vm');
var UgridClient = require('../lib/ugrid-io.js');
var ml = require('../lib/ugrid-ml.js')
var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['n', 'num=ARG', 'number of instances (default 1)'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var host = opt.options.Host || 'localhost';
var port = opt.options.Port || 12346;
var num = opt.options.num || 1;

if (cluster.isMaster) {
	for (var i = 0; i < num; i++)
		cluster.fork();
} else {
	runWorker(host, port);
}

function runWorker(host, port) {
	var grid = new UgridClient({
		host: host,
		port: port,
		data: {type: 'worker'}
	});

	var RAM = {}, STAGE_RAM = [], task;

	var request = {
		task: function(msg) {
			vm.runInThisContext('var Task = ' + msg.data.args.task);
			task = new Task(grid, ml, STAGE_RAM, RAM, msg.data.args.node, msg.data.args.action, function(res) {
				msg.cmd = 'reply';
				msg.id = msg.from;
				msg.data = res;
				grid.send_cb(msg, function(err2, res2) {
					if (err2) throw err2;
				});
			});
			task.run();
		},
		shuffle: function(msg) {
			task.processShuffle(msg.data.args);
		}
	};

	grid.connect_cb(function(err, res) {
		console.log('id: ' + res.id + ', uuid: ' + res.uuid);
		grid.host = {uuid: res.uuid, id: res.id};
		grid.on('request', function(msg) {
			try {request[msg.data.cmd](msg);}
			catch (error) {
				console.log(msg.data.fun + ' error : ' + error);
				msg.cmd = 'reply';
				msg.id = msg.from;
				msg.error = error;
				grid.send_cb(msg, function(err, res) {
					if (err) throw err;
				});
			}
		});
	});
}
