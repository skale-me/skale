#!/usr/local/bin/node

'use strict';

var cluster = require('cluster');
var vm = require('vm');
var readline = require('readline');
var fs = require('fs');

var UgridClient = require('../lib/ugrid-client.js');
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
	var RAM = {}, STAGE_RAM = [], task;

	var grid = new UgridClient({
		host: host,
		port: port,
		data: {type: 'worker'}
	});

	var request = {
		setTask: function(msg) {
			vm.runInThisContext('var Task = ' + msg.data.args.task);
			task = new Task(grid, fs, readline, ml, STAGE_RAM, RAM, msg.data.args.node, msg.data.args.action);
			grid.reply_cb(msg, null, 'worker ready to process task');
		},
		runTask: function(msg) {
			task.run(function(res) {
				grid.reply_cb(msg, null, res);
			});
		},
		shuffle: function(msg) {
			task.processShuffle(msg);
		}
	};

	grid.connect_cb(function(err, res) {
		console.log('id: ' + res.id + ', uuid: ' + res.uuid);
		grid.host = {uuid: res.uuid, id: res.id};
		grid.on('request', function(msg) {
			try {request[msg.data.cmd](msg);}
			catch (error) {
				console.log(msg.data.fun + ' error : ' + error);
				grid.reply_cb(msg, error, null);
			}
		});
	});
}
