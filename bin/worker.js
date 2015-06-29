#!/usr/bin/env node

'use strict';

var os = require('os');
var cluster = require('cluster');

var trace = require('line-trace');
var UgridClient = require('../lib/ugrid-client.js');
var RDD = require('../lib/ugrid-transformation.js');

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['d', 'debug', 'print debug traces'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var debug = opt.options.debug || false;
var ncpu = process.env.UGRID_WORKER_PER_HOST ? Number(process.env.UGRID_WORKER_PER_HOST) : os.cpus().length;
var cgrid;

if (cluster.isMaster) {
	cluster.on('exit', handleExit);
	cgrid = new UgridClient({
		debug: debug,
		host: opt.options.Host,
		port: opt.options.Port,
		data: {
			type: 'worker-controller',
			ncpu: ncpu
		}
	});
	cgrid.on('connect', function (msg) {
		for (var i = 0; i < ncpu; i++)
			cluster.fork({wsid: msg.wsid});
	});
	cgrid.on('getWorker', function (msg) {
		for (var i = 0; i < msg.n; i++)
			cluster.fork({wsid: msg.wsid});
	});
	console.log('worker controller ready');
} else {
	runWorker(opt.options.Host, opt.options.Port);
}

function handleExit(worker, code, signal) {
	console.log("worker %d exited: %s", worker.process.pid, signal || code);
}

function runWorker(host, port) {
	var jobs = {}, jobId;

	var grid = new UgridClient({
		debug: debug,
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
			wsid: process.env.wsid,
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
		setJob: function (msg) {
			// TODO: app object must be created once per application, and reset on worker release
			var worker = msg.data.args.worker;
			for (var wid = 0; wid < worker.length; wid++)
				if (worker[wid].uuid == grid.host.uuid) break;
			var app = {
				worker: worker,
				wid: wid,
				master_uuid: msg.data.master_uuid,
				dones: {},
				completedStreams: {}
			};
			jobs[msg.data.jobId] = new UgridJob(grid, app, {
				node: msg.data.args.node,
				stageData: msg.data.args.stageData,
				actionData: msg.data.args.actionData,
				jobId: msg.data.jobId
			});
			grid.reply(msg, null, 'worker ready to process job');
		},
		stream: function (msg) {
			if (msg.data.data === null) {
				grid.emit(msg.data.stream + ".end", done);
			} else {
				grid.emit(msg.data.stream, msg.data.data, done);
			}
			function done() {try {grid.reply(msg);} catch(err) {}}
		},
		block: function (msg) {
			grid.emit(msg.data.streamId + ".block", done);
			function done() {try {grid.reply(msg);} catch(err) {}}
		}
	};

	grid.on('remoteClose', function (msg) {
		process.exit(0);
	});

	grid.on('shuffle', function (msg) {
		try {jobs[msg.jobId].node[msg.shuffleNode].rx_shuffle(msg.args);}
		catch (err) {throw new Error("Lineage rx shuffle " + jobs[msg.jobId].node[msg.shuffleNode].type + ": " + err);}
		if (jobs[msg.jobId].node[msg.shuffleNode].stage.nShuffle == jobs[msg.jobId].app.worker.length)
			jobs[msg.jobId].node[msg.shuffleNode].runFromStageRam();
	});

	grid.on('runJob', function (msg) {
		jobs[msg.data.jobId].run();
	});

	grid.on('lastLine', function (msg) {
		jobs[msg.jobId].node[msg.args.targetNum].processLastLine(msg.args);
	});

	grid.on('action', function (msg) {
		jobs[msg.jobId].action.sendResult();		
	});

	grid.on('request', function (msg) {
		try {
			request[msg.data.cmd](msg);
		} catch (error) {
			console.error(error.stack);
			grid.reply(msg, error, null);
		}
	});
}

function UgridJob(grid, app, param) {
	this.id = param.jobId;
	this.node = param.node;
	this.app = app;
	this.action = new RDD[param.actionData.fun](grid, app, this, param.actionData);

	var nums = Object.keys(param.node).sort();
	for (var i = 0; i < nums.length; i++)
		this.node[nums[i]] = new RDD[this.node[nums[i]].type](grid, app, this, param.node[nums[i]]);

	var sources = [];
	function findSource(n) {
		if (n.inMemory || (n.child.length == 0)) {
			sources.push(n);
			return;
		}
		for (var i = 0; i < n.child.length; i++)
			findSource(n.child[i]);
	}
	findSource(this.node[nums[nums.length - 1]]);

	this.run = function() {
		treewalk(this.node[nums[nums.length - 1]], null,
		function c_out(n){
			if (n.inMemory || (n.child.length == 0)) {
				var tmp = n;
				while (tmp = tmp.anc) {
					n.nodePath.push(tmp);
					if (tmp.dependency == 'wide') break;
				}
			}
			if ((n.dependency == 'wide') && !n.inMemory) {
				var tmp = n;
				while (tmp = tmp.anc) {
					n.nodePath.push(tmp);
					if (tmp.dependency == 'wide') break;
				}
			}
		}, function end() {
			for (var i = 0; i < sources.length; i++) {
				if (sources[i].inMemory) sources[i].runFromRAM();
				else sources[i].run();
			}
		})
	};

	function treewalk(root, c_in, c_out, callback) {
		var n = root;
		if (c_in) c_in(n);
		while (1)
			if (n.child.length && (++n.visits <= n.child.length)) {
				n = n.child[n.visits - 1];
				if (c_in) c_in(n);
			} else {
				n.visits = 0;
				if (c_out) c_out(n);
				if (n == root) break;
				n = n.anc;
			}
		callback();
	}
};


