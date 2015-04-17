#!/usr/bin/env node

'use strict';

var os = require('os');
var cluster = require('cluster');

var UgridClient = require('../lib/ugrid-client.js');
var UgridTask = require('../lib/ugrid-processing.js').UgridTask;

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['n', 'num=ARG', 'number of instances (default 1)'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var num = opt.options.num || 1;

if (cluster.isMaster) {
	cluster.on('exit', handleExit);
	for (var i = 0; i < num; i++)
		cluster.fork();
} else {
	runWorker(opt.options.Host, opt.options.Port);
}

function handleExit(worker, code, signal) {
	console.log("worker %d died (%s). Restart.", worker.process.pid, signal || code);
	if (code != 2)
		cluster.fork();
}

function runWorker(host, port) {
	var RAM = {}, task, jobId;

	var grid = new UgridClient({
		host: host,
		port: port,
		data: {
			ncpu: os.cpus().length,
			os: os.type(),
			arch: os.arch(),
			usedmem: process.memoryUsage().rss,
			totalmem: os.totalmem(),
			hostname: os.hostname(),
			type: 'worker',
			jobId: ''
		}
	}, function (err, res) {
		console.log('id: ' + res.id + ', uuid: ' + res.uuid);
		grid.host = {uuid: res.uuid, id: res.id};
	});

	grid.on('error', function (err) {
		console.log("grid error %j", err);
		process.exit(2);
	});

	var request = {
		setTask: function (msg) {
			task = new UgridTask(grid, RAM, msg);
			grid.reply(msg, null, 'worker ready to process task');
		},
		runTask: function (msg) {
			task.run(function(res) {
				grid.reply(msg, null, res);
			}, msg);
		},
		shuffle: function (msg) {
			task.processShuffle(msg);
		},
		action: function (msg) {
			task.processAction(msg);
		},
		lastLine: function (msg) {
			task.processLastLine(msg);
		},
		reset: function () {
			if (!process.env.UGRID_TEST) process.exit(0);
			RAM = {};
			task = undefined;
			jobId = undefined;
		}
	};

	grid.on('request', function (msg) {
		try {
			request[msg.data.cmd](msg);
		} catch (error) {
			console.error(error.stack);
			grid.reply(msg, error, null);
		}
	});
}
