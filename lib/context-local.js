// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var child_process = require('child_process');
var mkdirp = require('mkdirp');
var uuid = require('node-uuid');
var dataset = require('./dataset.js');

var memory = 4096;

module.exports = Context;

function Context(args) {
	if (!(this instanceof Context))
		return new Context(args);
	this.id = 1;
	this.contextId = uuid.v4();
	var nworker = 3;
	var tmp = process.env.SKALE_TMP || '/tmp';
	var self = this;
	this.worker = new Array(nworker);
	for (var i = 0; i < nworker; i++) {
		this.worker[i] = new Worker(i);
	}
	this.jobId = 0;

	this.basedir = tmp + '/skale/' + this.contextId + '/';
	mkdirp.sync(this.basedir + 'tmp');
	mkdirp.sync(this.basedir + 'stream');

	this.datasetIdCounter = 0;	// global dataset id counter

	this.parallelize = function (localArray, nPartitions) { return dataset.parallelize(this, localArray, nPartitions);};
	this.range = function (start, end, step, nPartitions) { return dataset.range(this, start, end, step, nPartitions);};
	this.textFile = function (file, nPartitions) {return new dataset.TextFile(this, file, nPartitions);};
	this.lineStream = function (stream, config) {return new dataset.Stream(this, stream, 'line', config);};
	this.objectStream = function (stream, config) {return new dataset.Stream(this, stream, 'object', config);};

	this.runTask = function(task, callback) {
		function getLeastBusyWorkerId(/* preferredLocation */) {
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
		//var stream = new this.createReadStream(jobId, opt);	// user readable stream instance

		//this.getWorkers(function () {
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
		//});

		function runShuffleStage(stage, done) {
			findNodes(stage, function(nodes) {
				var pid = 0, shuffleTasks = [], cnt = 0, i, j;
				stage.shufflePartitions = [];

				for (i = 0; i < stage.dependencies.length; i++) {
					for (j = 0; j < stage.dependencies[i].partitions.length; j++)
						stage.shufflePartitions[pid++] = new dataset.Partition(stage.id, pid, stage.dependencies[i].id, stage.dependencies[i].partitions[j].partitionIndex);
				}

				for (i = 0; i < stage.shufflePartitions.length; i++)
					shuffleTasks.push(new Task(self.basedir, jobId, nodes, stage.id, i));

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
				for (var i = 0; i < root.partitions.length; i++) {
					tasks.push(new Task(self.basedir, jobId, nodes, root.id, i, action));
				}
				//callback({id: jobId, stream: stream}, tasks);
				callback({id: jobId}, tasks);
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

//		return stream;
	};

	function rpc(cmd, workerNum, args, callback) {
		//self.request(self.worker[workerNum], {cmd: cmd, args: args, master_uuid: self.uuid, worker: self.worker}, callback);
		self.worker[workerNum].request({cmd: cmd, args: args}, callback);
	}
}

Context.prototype.log = log;

Context.prototype.info = function() {
	return this.worker;
};

Context.prototype.end = function () {
	for (var i = 0; i < this.worker.length; i++) {
		this.worker[i].child.disconnect();
	}
};

function Worker(index) {
	this.index = index;
	this.reqid = 1;
	this.reqcb = {};
	this.ntask = 0;
	this.child = child_process.fork(__dirname + '/worker-local.js', [index, memory], {
		execArgv: ['--max_old_space_size=' + memory]
	});
	var self = this;

	log('start worker', index);
	this.child.on('message', function(msg) {
		if (self.reqcb[msg.id]) {
			self.reqcb[msg.id](msg.error, msg.result);
			delete self.reqcb[msg.id];
		}
	});
}

Worker.prototype.send = function(msg) {
	this.child.send(msg);
};

Worker.prototype.request = function (req, done) {
	this.reqcb[this.reqid] = done;
	this.child.send({id: this.reqid++, req: req});
};

function Task(basedir, jobId, nodes, datasetId, pid, action) {
	this.basedir = basedir;
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

		this.lib.mkdirp.sync(this.basedir + 'shuffle');

		if (action) {
			if (action.foreach) {
				pipeline.push({transform: function foreach(context, data) {
					for (var i = 0; i < data.length; i++) action.src(data[i]);
				}});
			} else {
				pipeline.push({transform: function aggregate(context, data) {
					for (var i = 0; i < data.length; i++)
						action.init = action.src(action.init, data[i]);
				}});
			}
		}

		for (;;) {
			var tmpPartAvailable = mm.isAvailable(tmpPart);							// is partition available in memory
			if (!tmpPartAvailable && tmpDataset.persistent)	{							// if data must be stored in memory
				if ((action != undefined) || (tmpDataset.id != this.datasetId)) { 			// no persist if no action and shuffleRDD
					blocksToRegister.push(tmpPart);									// register block inside memory manager
					pipeline.unshift(tmpPart);										// add it to pipeline
					tmpPart.mm = this.mm;
				}
			}
			if (tmpPartAvailable || (tmpPart.parentDatasetId == undefined)) break;		// source partition found
			pipeline.unshift(tmpDataset);												// else add current dataset transform to pipeline
			tmpPart = this.nodes[tmpPart.parentDatasetId].partitions[tmpPart.parentPartitionIndex];
			tmpDataset = this.nodes[tmpPart.datasetId];
		}

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

function log() {
	var args = Array.prototype.slice.call(arguments);
	args.unshift('[master]');
	console.log.apply(null, args);
}
