#!/usr/bin/env node

'use strict';

var os = require('os');
var cluster = require('cluster');

var UgridClient = require('../lib/ugrid-client.js');
var UgridJob = require('../lib/ugrid-processing.js').UgridJob;

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
	var RAM = {}, job, jobId;

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

	grid.on('runJob', function (err) {
		job.run();
	});

	var request = {
		setJob: function (msg) {
			job = new UgridJob(request, grid, RAM, msg);
			grid.reply(msg, null, 'worker ready to process job');
		},
		// runJob: function (msg) {
		// 	job.run(function(res) {
		// 		grid.reply(msg, null, res);
		// 	}, msg);
		// },
		shuffle: function (msg) {
			// Ici job doit etre un JSON indexé par l'id du job
			// msg doit contenir l'id du job concerné par la request
			// afin de pouvoir ecrire job[msg.data.jobid].processShuffle(msg);
			job.processShuffle(msg);
		},
		action: function (msg) {
			// idem shuffle
			job.processAction(msg);
		},
		lastLine: function (msg) {
			// idem shuffle
			job.processLastLine(msg);
		},
		reset: function () {
			if (!process.env.UGRID_TEST) process.exit(0);
			RAM = {};
			job = undefined;
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
