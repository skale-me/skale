#!/usr/bin/env node

'use strict';

var fs = require('fs');
var os = require('os');
var cluster = require('cluster');
var Ssh2 = require('ssh2');
var uuid = require('node-uuid');

var UgridClient = require('../lib/ugrid-client.js');
var ml = require('../lib/ugrid-ml.js');
var trace = require('line-trace');
var Lines = require('../lib/lines.js');
var sizeOf = require('../utils/sizeof.js');
var readSplit = require('../utils/readsplit.js').readSplit;

var global = {require: require};
var mm = new MemoryManager();

var opt = require('node-getopt').create([
	['h', 'help', 'print this help text'],
	['d', 'debug', 'print debug traces'],
	['m', 'MyHost=ARG', 'advertised hostname'],
	['n', 'Num=ARG', 'number of workers (default: number of cpus)'],
	['H', 'Host=ARG', 'server hostname (default localhost)'],
	['P', 'Port=ARG', 'server port (default 12346)']
]).bindHelp().parseSystem();

var debug = opt.options.debug || false;
var ncpu = opt.options.Num || (process.env.UGRID_WORKER_PER_HOST ? process.env.UGRID_WORKER_PER_HOST : os.cpus().length);
var hostname = opt.options.MyHost || os.hostname();
var cgrid;

ncpu = Number(ncpu);
var sshUser = process.env.USER;
var sshPrivateKey = fs.readFileSync(process.env.HOME + '/.ssh/id_rsa');
var ssh = {};
var sftp = {};
var transferQueue = [];		// File transfer queue, on controller, to serialize concurrent worker requests during init
var ftid = 0;				// File transfer id, set on worker to handle controller responses.
var ftcb = {};				// File transfer callback, on worker, indexed by ftid.

// On controller, file transfer function
function scp(msg, done) {
	if (sftp[msg.from])			// ssh ok and sftp session ready
		return sftp[msg.from].fastGet(msg.remote, msg.local, {}, done);

	// sftp session not yet established, enqueue the request, init session once
	transferQueue.push([msg.remote, msg.local, done]);
	if (!ssh[msg.from]) {		// ssh connection not yet established
		var cnx = ssh[msg.from] = new Ssh2();
		cnx.connect({
			host: msg.from,
			username: sshUser,
			privateKey: sshPrivateKey
		});
		cnx.on('ready', function () {
			cnx.sftp(function (err, res) {
				if (err) {
					for (var i = 0; i < transferQueue.length; i++)
						transferQueue[i][2](err);
					return;
				}
				sftp[msg.from] = res;
				transferQueue.forEach(function (req) {
					res.fastGet(req[0], req[1], {}, req[2]);
				});
			});
		});
	}
}

if (cluster.isMaster) {
	cluster.on('exit', handleExit);
	cgrid = new UgridClient({
		debug: debug,
		host: opt.options.Host,
		port: opt.options.Port,
		data: {
			type: 'worker-controller',
			hostname: hostname,
			ncpu: ncpu
		}
	});
	cgrid.on('connect', startWorkers);
	cgrid.on('getWorker', startWorkers);
	cgrid.on('close', process.exit);
	console.log('worker controller ready');
} else {
	runWorker(opt.options.Host, opt.options.Port);
}

function startWorkers(msg) {
	var worker = [];
	var n = msg.n || ncpu;
	for (var i = 0; i < n; i++)
		worker[i] = cluster.fork({wsid: msg.wsid});
	worker.forEach(function (w) {
		w.on('message', function (msg) {
			switch (msg.cmd) {
			case 'scp':
				scp(msg, function (err, res) {w.send({ftid: msg.ftid, err: err, res: res});});
				break;
			default:
				console.log('unexpected msg %j', msg);
			}
		});
	});
}

function handleExit(worker, code, signal) {
	console.log("worker pid %d exited: %s", worker.process.pid, signal || code);
}

// On worker, file transfer function: send a request to controller, handle
// response in callback
function transfer(host, remote, local, done) {
	ftcb[ftid] = done;
	process.send({cmd: 'scp', from: host, remote: remote, local: local, ftid: ftid++});
}

function runWorker(host, port) {
	var jobs = {};

	process.on('uncaughtException', function (err) {
		grid.send(grid.muuid, {cmd: 'workerError', args: err.stack});
		process.exit(2);
	});

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
			hostname: hostname,
			type: 'worker',
			wsid: process.env.wsid,
			jobId: ''
		}
	}, function (err, res) {
		console.log('id: ' + res.id + ', uuid: ' + res.uuid);
		grid.host = {uuid: res.uuid, id: res.id};
		grid.workerHost = {};
	});

	grid.on('error', function (err) {
		console.log("grid error %j", err);
		process.exit(2);
	});

	var request = {
		runTask: function runTask(msg) {
			grid.muuid = msg.data.master_uuid;
			var task = uc_parse(msg.data.args);
			task.load({mm: mm, sizeOf: sizeOf, fs: fs, ml: ml, readSplit: readSplit, Lines: Lines, task: task, uuid: uuid, grid: grid});
			task.run(function(result) {grid.reply(msg, null, result);});
		}
	};

	grid.on('remoteClose', process.exit);

	grid.on('request', function (msg) {
		if (msg.first) {
			for (var i = 0; i < msg.first.length; i++)
				grid.workerHost[i] = msg.first[i].hostname;
		}
		try {request[msg.data.cmd](msg);} 
		catch (error) {
			console.error(error.stack);
			grid.reply(msg, error, null);
		}
	});

	grid.on('sendFile', function (msg) {
		fs.createReadStream(msg.path).pipe(grid.createStreamTo(msg));
	});

	// Handle messages from worker controller (replies to scp requests)
	process.on('message', function (msg) {
		if (!ftcb[msg.ftid]) {
			console.error('no callback found: %j', msg);
			return;
		}
		ftcb[msg.ftid](msg.err, msg.res);
		ftcb[msg.ftid] = undefined;
	});
}

function MemoryManager() {
	var Kb = 1024, Mb = 1024 * Kb, Gb = 1024 * Mb;
	var MAX_MEMORY = 1.0 * Gb;							// To bet set as max_old_space_size value when launching node
	var maxStorageMemory = MAX_MEMORY * 0.4;
	var maxShuffleMemory = MAX_MEMORY * 0.2;
	var maxCollectMemory = MAX_MEMORY * 0.2;

	this.storageMemory = 0;
	this.shuffleMemory = 0;
	this.collectMemory = 0;

	this.storageFull = function() {return (this.storageMemory > maxStorageMemory);}
	this.shuffleFull = function() {return (this.shuffleMemory > maxShuffleMemory);}
	this.collectFull = function() {return (this.collectMemory > maxCollectMemory);}

	this.partitions = {};
	this.register = function(partition) {
		var key = partition.RDDId + '.' + partition.partitionIndex;
		if (this.partitions[key]) console.log('Partition already exists')
		else {
			console.log('registering with key: ' + key)
			this.partitions[key] = partition;
		}
	}

	this.isAvailable = function(partition) {
		return (this.partitions[partition.RDDId + '.' + partition.partitionIndex] != undefined);
	}
}

function uc_parse(str) {
	function recompile(s) {
		if (s.indexOf('=>') >= 0) return eval(s);	// Support arrow functions
		var args = s.match(/\(([^)]*)/)[1];
		var body = s.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
		return new Function(args, body);
	}
	return JSON.parse(str,function(key, value) {
		if (typeof value != 'string') return value;
		return (value.substring(0, 8) == 'function') ? recompile(value) : value;
	});
}
