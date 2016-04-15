// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var util = require('util');
var uuid = require('node-uuid');
var fs = require('fs');

var SkaleClient = require('./client.js');
var dataset = require('./dataset.js');
var mkdir = require('./mkdir.js');

module.exports = SkaleContext;

util.inherits(SkaleContext, SkaleClient);

function SkaleContext(arg) {
	if (!(this instanceof SkaleContext))
		return new SkaleContext(arg);
	var self = this;

	arg = arg || {};
	arg.data = arg.data || {};
	arg.data.type = 'master';
	SkaleClient.call(this, arg);
	var maxWorker = process.env.SKALE_MAX_WORKER;

	this.started = this.ended = false;
	this.jobId = 0;
	this.firstData = {};
	this.worker = [];
	this.contextId = uuid.v4();	// context id which will be used as scratch directory name

	this.basedir = '/tmp/skale/' + this.contextId + '/';
	mkdir(this.basedir + 'tmp');
	mkdir(this.basedir + 'stream');

	this.once('connect', function(data) {
		var i;
		if (!maxWorker || maxWorker > data.devices.length)
			maxWorker = data.devices.length;
		for (i = 0; i < maxWorker; i++) {
			self.worker.push(new Worker(data.devices[i]));
			self.firstData[data.devices[i].uuid] = data.devices;
		}
		self.started = true;
	});

	this.on('workerError', function workerError(msg) {
		console.error('Error from worker id %d:', msg.from);
		console.error(msg.args);
	});

	this.on('remoteClose', function getWorkerClose() {
		throw 'Fatal error: unexpected worker exit';
	});

	this.getWorkers = function (callback) {
		if (self.started) return callback();
		this.once('connect', function() {callback();});
	};

	function Worker(w) {
		this.uuid = w.uuid;
		this.id = w.id;
		this.ip = w.ip;
		this.ntask = 0;
	}

	Worker.prototype.rpc = function (cmd, args, done) {
		self.request({uuid: this.uuid, id: this.id}, {cmd: cmd, args: args}, done);
	};

	Worker.prototype.send = function (cmd, args) {
		self.send(this.uuid, {cmd: cmd, args: args});
	};

	this.on('request', function (msg) { // Protocol to handle stream flow control: reply when data is consumed
		if (msg.data.cmd == 'stream') {
			self.emit(msg.data.stream, msg.data.data, function() {
				try {self.reply(msg);} catch(err) {}
			});
		}
	});

	this.on('sendFile', function (msg) {
		fs.createReadStream(msg.path, msg.opt).pipe(self.createStreamTo(msg));
	});

	this.end = function () {
		if (self.ended) return;
		self.ended = true;
		if (this.started) self.set({complete: 1});
		self._end();
	};

	this.datasetIdCounter = 0;	// global dataset id counter

	this.parallelize = function (localArray, nPartitions) { return dataset.parallelize(this, localArray, nPartitions);};
	this.range = function (start, end, step, nPartitions) { return dataset.range(this, start, end, step, nPartitions);};
	this.textFile = function (file, nPartitions) {return new dataset.TextFile(this, file, nPartitions);};
	this.lineStream = function (stream, config) {return new dataset.Stream(this, stream, 'line', config);};
	this.objectStream = function (stream, config) {return new dataset.Stream(this, stream, 'object', config);};

	this.runTask = function(task, callback) {
		function getLeastBusyWorkerId(preferredLocation) {
			var wid, ntask;
			for (var i = 0; i < self.worker.length; i++) {
				if ((ntask == undefined) || (ntask > self.worker[i].ntask)) {
					ntask = self.worker[i].ntask;
					wid = i;
				}
			}
			return wid;
		}

		function serialize(obj) {
			return JSON.stringify(obj,function(key, value) {
				if (key == 'sc') return undefined;
				return (typeof value === 'function' ) ? value.toString() : value;
			});
		}

		var wid = getLeastBusyWorkerId(task.nodes[task.datasetId].getPreferedLocation(task.pid));
		this.worker[wid].ntask++;
		rpc('runTask', wid, serialize(task), function(err, res) {
			self.worker[wid].ntask--;
			callback(err, res);
		});
	};

	this.runJob = function(opt, root, action, callback) {
		var jobId = this.jobId++;
		var stream = opt._stream || new this.createReadStream(jobId, opt);	// user redable stream instance

		this.getWorkers(function () {
			findShuffleStages(function(shuffleStages) {
				if (shuffleStages.length == 0) runResultStage();
				else {
					var cnt = 0;
					runShuffleStage(shuffleStages[cnt], shuffleDone);
				}
				function shuffleDone() {
					if (++cnt < shuffleStages.length) runShuffleStage(shuffleStages[cnt], shuffleDone);
					else runResultStage();
				}
			});
		});

		function runShuffleStage(stage, done) {
			findNodes(stage, function(nodes) {
				var pid = 0, shuffleTasks = [], cnt = 0, i, j;
				stage.shufflePartitions = [];

				for (i = 0; i < stage.dependencies.length; i++) {
					for (j = 0; j < stage.dependencies[i].partitions.length; j++)
						stage.shufflePartitions[pid++] = new dataset.Partition(stage.id, pid, stage.dependencies[i].id, stage.dependencies[i].partitions[j].partitionIndex);
				}

				for (i = 0; i < stage.shufflePartitions.length; i++)
					shuffleTasks.push(new Task(self.contextId, jobId, nodes, stage.id, i));

				for (i = 0; i < shuffleTasks.length; i++) {
					self.runTask(shuffleTasks[i], function (err, res) {
						stage.shufflePartitions[res.pid].files = res.files;
						if (++cnt < shuffleTasks.length) return;
						stage.executed = true;
						done();
					});
				}
			});
		}

		function runResultStage() {
			findNodes(root, function(nodes) {
				var tasks = [];
				for (var i = 0; i < root.partitions.length; i++)
					tasks.push(new Task(self.contextId, jobId, nodes, root.id, i, action));
				callback({id: jobId, stream: stream}, tasks);
			});
		}

		function findNodes(node, done) {
			var nodes = {};
			interruptibleTreewalk(node, function cin(n, done) {
				done(n.shuffling && n.executed);
			}, function cout(n, done) {
				n.getPartitions(function() {
					if (nodes[n.id] == undefined) nodes[n.id] = n;
					done();
				});
			}, function() {done(nodes);});
		}

		function findShuffleStages(callback) {
			var stages = [];
			interruptibleTreewalk(root, function cin(n, done) {
				if (n.shuffling && !n.executed) stages.unshift(n);
				done(n.shuffling && n.executed); 	// stage boundary are shuffle nodes
			}, function cout(n, done) {done();}, function() {callback(stages);});
		}

		function interruptibleTreewalk(n, cin, cout, done) {
			cin(n, function(uturn) { // if uturn equals true the subtree under node won't be treewalked
				if (!uturn) {
					var nDependencies = 0;
					for (var i = 0; i < n.dependencies.length; i++)
						interruptibleTreewalk(n.dependencies[i], cin, cout, function() {
							if (++nDependencies == n.dependencies.length) cout(n, done);
						});
					if (n.dependencies.length == 0) cout(n, done);
				} else cout(n, done);
			});
		}

		return stream;
	};

	function rpc(cmd, workerNum, args, callback) {
		self.request(self.worker[workerNum], {cmd: cmd, args: args, master_uuid: self.uuid, worker: self.worker}, callback);
	}
}

function Task(contextId, jobId, nodes, datasetId, pid, action) {
	this.id = jobId;					// cette variable est surement inutile
	this.contextId = contextId;
	this.datasetId = datasetId;
	this.pid = pid;
	this.nodes = nodes;
	this.action = action;
	this.files = {};			// object in which we store shuffle file informations to be sent back to master
	this.lib;					// handler to libraries required on worker side (which cannot be serialized)
	this.mm;					// handler to worker side memory manager instance 
	this.grid;					// handler to socket object instance

	this.run = function(done) {
		var pipeline = [], self = this, mm = this.mm, action = this.action;
		var p = this.pid;
		var tmpPart = action ? this.nodes[this.datasetId].partitions[p] : this.nodes[this.datasetId].shufflePartitions[p];
		var tmpDataset = this.nodes[tmpPart.datasetId];
		var blocksToRegister = [];

		this.basedir = '/tmp/skale/' + this.contextId + '/';
		this.lib.mkdir(this.basedir + 'shuffle');

		if (action)
			pipeline.push({transform: function aggregate(context, data) {
				for (var i = 0; i < data.length; i++)
					action.init = action.src(action.init, data[i]);
			}});

		do {
			var tmpPartAvailable = mm.isAvailable(tmpPart);							// is partition available in memory
			if (!tmpPartAvailable && tmpDataset.persistent)	{							// if data must be stored in memory
				if ((action != undefined) || (tmpDataset.id != this.datasetId)) { 			// no persist if no action and shuffleRDD
					blocksToRegister.push(tmpPart);									// register block inside memory manager
					pipeline.unshift(tmpPart);										// add it to pipeline
				}
			}
			if (tmpPartAvailable || (tmpPart.parentDatasetId == undefined)) break;		// source partition found
			pipeline.unshift(tmpDataset);												// else add current dataset transform to pipeline
			tmpPart = this.nodes[tmpPart.parentDatasetId].partitions[tmpPart.parentPartitionIndex];
			tmpDataset = this.nodes[tmpPart.datasetId];
		} while (1);

		if (tmpPartAvailable) mm.partitions[tmpPart.datasetId + '.' + tmpPart.partitionIndex].iterate(this, tmpPart.partitionIndex, pipeline, iterateDone);
		else this.nodes[tmpPart.datasetId].iterate(this, tmpPart.partitionIndex, pipeline, iterateDone);

		function iterateDone() {
			blocksToRegister.map(function(block) {mm.register(block);});
			if (action) done({data: action.init});
			else self.nodes[self.datasetId].spillToDisk(self, function() {
				done({pid: self.pid, files: self.files});
			});
		}
	};

	this.getReadStream = function (fileObj, opt) {
		var fs = this.lib.fs;
		try {
			fs.accessSync(fileObj.path);
			return fs.createReadStream(fileObj.path, opt);
		} catch (err) {
			if (!fileObj.host) fileObj.host = this.grid.muuid;
			return this.grid.createStreamFrom(fileObj.host, {cmd: 'sendFile', path: fileObj.path, opt: opt});
		}
	};
}
