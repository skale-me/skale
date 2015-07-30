'use strict';

var fs = require('fs');

var ml = require('./ugrid-ml.js');
var trace = require('line-trace');
var Lines = require('./lines.js');
var Lines2 = require('./lines2.js');

/*
DONE:
	- parallelize, randomSVMData (any number of partitions)
	- collect, reduce
	- persist (NB: avec régénération des partitions non stockées)
	- narrow: union
	- wide: groupByKey, reduceByKey
	- RDD.collect(), RDD.pipeline(),  RDD.run(), Partition.iterate() sont asynchrones 
	- Benchmark: logreg
	- single worker textFile
TODO:
	- textFile multiworker

NB:
	- restaurer l'ordre des partitions dans le cas du collect (est-ce vraiment necessaire ?)
	- spawner les RDD dans l'ordre top vers bottom permet d'identifier les stages immédiatement
	- dans RDD.pipeline() faire une do while pour factoriser du code
	- textFile fonctionne avec 1 seule partition par worker
*/

function recompile(s) {
	var args = s.match(/\(([^)]*)/)[1];
	var body = s.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
	return new Function(args, body);
}

function Partition(source, data, key) {
	this.source = source;		// {id: 1234, p: 0}
	this.data = data;
	this.key = key;
	var self = this;	// class method are called with a different this

	this.save = function(array) {
		for (var i = 0; i < array.length; i++)
			self.data.push(array[i]);
		return array;
	}

	this.iterate = function(pipeline, done, callback) {
		var buffer;
		for (var i = 0; i < self.data.length; i++) {
			buffer = [self.data[i]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t](buffer);
			callback && callback(buffer);
		}
		done();
	}
}

function randomSVMDataRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var D = param.args[0];
	var partitions = param.args[1] || [];

	this.partitions = [];
	for (var p = 0; p < partitions.length; p++)
		this.partitions.push(new Partition(null));

	this.iterate = function(p, pipeline, done, callback) {
		var rng = new ml.Random(partitions[p].seed), buffer;

		for (var i = 0; i < partitions[p].n; i++) {
			buffer = [ml.randomSVMLine(rng, D)];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t](buffer);
			callback && callback(buffer);
		}
		done();
	}
}

function textFileRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);	

	var self = this;
 	var blocks = param.args[2];
	var hashedBlocks = {};
	var blockIdx = 0;

	this.partitions = [];
	for (var p = 0; p < blocks.length; p++)
		this.partitions.push(new Partition(null));

	for (var i = 0; i < blocks.length; i++) {
		blocks[i].p = i;
		hashedBlocks[blocks[i].bid] = blocks[i];
	}

	this.iterate = function(p, pipeline, done, callback) {
		if (blocks.length == 0) done();
		else processBlock(p, pipeline, done, callback);
	}

	function processLine(p, line, first, pipeline, callback) {
		var buffer = [line];
		for (var t = 0; t < pipeline.length; t++)
			buffer = pipeline[t](buffer);
		callback && callback(buffer);		
	}

	function processBlock(bid, pipeline, done, callback) {
		// temporaire ici
		self.pipeline = pipeline;
		self.callback = callback;
		self.done = done;

		var lines = new Lines(), buffer;
		fs.createReadStream(blocks[bid].file, blocks[bid].opt).pipe(lines);

		lines.once("data", function (line) {
			blocks[bid].firstLine = line;
			lines.once("data", function (line) {
				blocks[bid].lastLine = line;
				lines.on("data", function (line) {
					buffer = [blocks[bid].lastLine];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t](buffer);
					callback && callback(buffer);
					blocks[bid].lastLine = line;					
				});
			});
		});

		function shuffleLine(shuffledLine) {
			var shuffleTo = blocks[bid].shuffleTo;
			if (shuffleTo != app.wid) {
				grid.send(app.worker[shuffleTo].uuid, {
					cmd: 'lastLine', 
					args: {lastLine: shuffledLine, rddId: self.id, bid: blocks[bid].bid + 1}, 
					jobId: job.id
				});
			} else processLastLine({lastLine: shuffledLine, bid: blocks[bid].bid + 1});
		}

		lines.on("endNewline", function(lastLineComplete) {
			var firstLineProcessed;
			var isFirstBlock = (blocks[bid].skipFirstLine === false);
			var isLastBlock = (blocks[bid].shuffleLastLine === false);
			var hasLastLine = blocks[bid].lastLine !== undefined;

			blocks[bid].hasScannedFile = true;
			// FIRST LINE
			if (isFirstBlock) {
				firstLineProcessed = true;
				if (hasLastLine || lastLineComplete || isLastBlock)
					processLine(blocks[bid].p, blocks[bid].firstLine, true, pipeline, callback);
				if (!hasLastLine && !lastLineComplete && !isLastBlock)
					shuffleLine(blocks[bid].firstLine);
			} else {
				blocks[bid].forward = (!hasLastLine && !lastLineComplete) ? true : false;
				firstLineProcessed = processFirstLine(blocks[bid].bid);
			}
			// LAST LINE
			if (hasLastLine) {
				if (isLastBlock) processLine(blocks[bid].p, blocks[bid].lastLine, undefined, pipeline, callback);
				else if (lastLineComplete) {
					processLine(blocks[bid].p, blocks[bid].lastLine, undefined, pipeline, callback);
					shuffleLine('');
				} else shuffleLine(blocks[bid].lastLine);
			} else if (lastLineComplete && !isLastBlock) shuffleLine('');

			if (firstLineProcessed) done();
		});
	}

	var processLastLine = this.processLastLine = function(data) {
		var targetBlock = hashedBlocks[data.bid];
		targetBlock.rxLastLine = data.lastLine;
		targetBlock.hasReiceivedLastLine = true;
		var firstLineProcessed = processFirstLine(data.bid);

		if (firstLineProcessed) self.done();
	};

	function processFirstLine(bid) {
		var targetBlock = hashedBlocks[bid];
		if (!targetBlock.hasReiceivedLastLine || !targetBlock.hasScannedFile) return false;
		if (targetBlock.forward && targetBlock.shuffleLastLine) {
			var shuffledLine = targetBlock.rxLastLine + targetBlock.firstLine;
			if (targetBlock.shuffleTo != app.wid) {
				grid.send(app.worker[targetBlock.shuffleTo].uuid, {
					cmd: 'lastLine', 
					args: {lastLine: shuffledLine, rddId: self.id, bid: bid + 1}
				});
			} else processLastLine({lastLine: shuffledLine, bid: bid + 1});
		} else {
			var str = (targetBlock.rxLastLine === undefined) ? targetBlock.firstLine : targetBlock.rxLastLine + targetBlock.firstLine;
			processLine(targetBlock.p, str, true, self.pipeline, self.callback);
		}
		return true;
	}
}


function RDD(grid, app, job, param) {
	this.id = param.id;
	this.dependencies = param.dependencies;
	this.persistent = param.persistent;
	this.partitions;
	this.nShuffle = 0;
	this.nTargetShuffle = app.worker.length;
	this.nextStage;
	this.type = param.type;

	var self = this;

	function findPartitions(n) {
		if (n.partitions == undefined) {
			n.partitions = [];
			for (var i = 0; i < n.dependencies.length; i++) {
				var parentPartitions = findPartitions(job.rdd[n.dependencies[i]]);
				for (var p = 0; p < parentPartitions.length; p++)
					n.partitions.push(new Partition({id: n.dependencies[i], p: p}));
			}
		}
		return n.partitions;
	}

	this.reduce = function(callback, param) {
		var result = JSON.parse(JSON.stringify(param.args[0])), p = -1;
		var reducer = recompile(param.src);

		function reduce() {
			if (++p < self.partitions.length) {
				self.pipeline(p, reduce, function (data) {
					for (var i = 0; i < data.length; i++)
						result = reducer(result, data[i]);
				});
			} else callback(result);
		}

		findPartitions(this);
		reduce();
	};

	// Attention à la gestion de la mise en tete de la data dans le cas du textFile
	this.collect = function(callback) {
		var result = {}, p = -1;

		function collect() {
			if (++p < self.partitions.length) {
				result[p] = [];
				self.pipeline(p, collect, function (data) {
					for (var i = 0; i < data.length; i++) result[p].push(data[i]);
				});
			} else callback(result);
		}

		findPartitions(this);
		collect();
	}

	this.run = function(callback) {
		this.nextStage = callback;
		// ICI il faut stocker la callback dans le this du dataset pour l'appeler de façon asynchrone à la fin de tous les rx_shuffle 
		var p = -1;
		function run() {
			if (++p < self.partitions.length) self.pipeline(p, run);
			else {
				for (var i = 0; i < self.map.length; i++)
					if (grid.host.uuid == app.worker[i].uuid) self.rx_shuffle(self.map[i]);
					else grid.send(app.worker[i].uuid, {cmd: 'shuffle', args: self.map[i], jobId: job.id, rddId: self.id});
			}
		}

		findPartitions(this);
		run();
	}

	this.pipeline = function(p, done, callback) {
		var pipeline = [];
		var sourceRDD = self;
		var sourcePartitionIdx = p;
		var sourcePartition = self.partitions[p];

		// Build p-th partition pipeline
		while ((sourcePartition.data == undefined) && sourcePartition.source) {
			if (sourceRDD.persistent && (p == 0)) {								// DEBUG partial RAM storage, remove (p == 0) for target version
				sourcePartition.data = [];
				pipeline.unshift(sourcePartition.save);
			}
			if (sourceRDD.transform) pipeline.unshift(sourceRDD.transform);
			sourceRDD = job.rdd[sourcePartition.source.id];
			sourcePartitionIdx = sourcePartition.source.p;
			sourcePartition = sourceRDD.partitions[sourcePartitionIdx];
		}
		if (sourcePartition.data) sourcePartition.iterate(pipeline, done, callback);
		else {
			if (sourceRDD.persistent && (p == 0)) {	// Add source partition save if needed
				sourcePartition.data = [];
				pipeline.unshift(sourcePartition.save);
			}
			sourceRDD.iterate(sourcePartitionIdx, pipeline, done, callback);
		}
	}

	if (this.persistent) app.rdd[param.id] = this;
}

module.exports.UgridJob = function(grid, app, param) {
	this.id = param.jobId;
	this.sendResultSemaphore = 0;
	this.rdd = {};

	var stream = grid.createWriteStream(this.id, app.master_uuid), stageBoundaries = [], stages = [], self = this;

	// Step 1: Spawn RDDs from top to bottom
	for (var i = 0; i < param.node.length; i++) {
		if (app.rdd[param.node[i].id] == undefined) {
			switch (param.node[i].type) {
			case 'parallelize': this.rdd[param.node[i].id] = new parallizedRDD(grid, app, this, param.node[i]); break;
			case 'randomSVMData': this.rdd[param.node[i].id] = new randomSVMDataRDD(grid, app, this, param.node[i]); break;
			case 'textFile': this.rdd[param.node[i].id] = new textFileRDD(grid, app, this, param.node[i]); break;
			case 'map': this.rdd[param.node[i].id] = new mappedRDD(grid, app, this, param.node[i]); break;
			case 'union': this.rdd[param.node[i].id] = new unionedRDD(grid, app, this, param.node[i]); break;
			case 'groupByKey': this.rdd[param.node[i].id] = new groupedByKeyRDD(grid, app, this, param.node[i]); break;
			case 'reduceByKey': this.rdd[param.node[i].id] = new reducedByKeyRDD(grid, app, this, param.node[i]); break;
			default: console.error('Not yet implemented');
			}
		} else this.rdd[param.node[i].id] = app.rdd[param.node[i].id];
		if (this.rdd[param.node[i].id].shuffling)
			stageBoundaries.unshift(this.rdd[param.node[i].id]);
	}

	// Step 2: Find stages
	for (var i = 0; i < stageBoundaries.length; i++) {
		if (stageBoundaries[i].partitions) break;
		stages.unshift(stageBoundaries[i]);
	}

	this.sendResult = function() {
		if ((app.wid != 0) && (++this.sendResultSemaphore != 2)) return;
		stream.write(this.result);
		if (app.worker[app.wid + 1])
			grid.send(app.worker[app.wid + 1].uuid, {cmd: 'action', jobId: this.id});
		grid.send(app.master_uuid, {cmd: 'endJob', data: this.id});
	}

	this.run = function() {
		var root = this.rdd[param.node[param.node.length - 1].id], s = -1;
		function exec() {
			if (++s < stages.length) stages[s].run(exec);
			else {
				root[param.action.fun](function(res) {
					self.result = res;
					self.sendResult();
				}, param.action);
			}
		}
		exec();
	}

	// // Set flush semaphores
	// this.setSemaphore = function(stageNode, n) {
	// 	if (n.inMemory || (n.dependency == 'wide') || (n.child.length == 0)) {
	// 		if (stageNode.sourceNodes.indexOf(n.num) == -1) {
	// 			stageNode.sourceNodes.push(n.num);
	// 			stageNode.targetPathCount++;
	// 			stageNode.nextTargetPathCount = stageNode.targetPathCount;
	// 		}
	// 		return;
	// 	}
	// 	for (var i = 0; i < n.child.length; i++) this.setSemaphore(stageNode, n.child[i]);
	// }
	// this.setSemaphore(this.action, this.node[root]);
	// for (var i = 0; i < stageNodeNums.length; i++)
	// 	for (var j = 0; j < this.node[stageNodeNums[i]].child.length; j++)
	// 		this.setSemaphore(this.node[stageNodeNums[i]], this.node[stageNodeNums[i]].child[j]);
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 

function reducedByKeyRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	this.shuffling = true;
	var self = this;
	var map = this.map = app.worker.map(function() {return {};});
	var SRAM = [];

	var reducer = recompile(param.src);
	var initVal = param.args[0];

	this.transform = function(data) {
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined)
				map[wid][key] = [[key, JSON.parse(JSON.stringify(initVal))]];
			map[wid][key][0][1] = reducer(map[wid][key][0][1], data[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++) {
				if (SRAM[i].key == key)  {
					SRAM[i].data[0][1] = reducer(SRAM[i].data[0][1], data[key[0][1]]);
					break;
				}
			}
			if (i == SRAM.length) SRAM.push(new Partition(null, data[key], key));
		}
		if (++self.nShuffle < self.nTargetShuffle) return;
		self.partitions = SRAM;
		self.nextStage();
	};
}

function groupedByKeyRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	this.shuffling = true;
	var self = this;
	var map = this.map = app.worker.map(function() {return {};});
	var SRAM = [];

	this.transform = function(data) {
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined)		// ATTENTION car la clé 1 et "1" ira dans la meme partition
				map[wid][key] = [[key, []]];
			map[wid][key][0][1].push(data[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++) {
				if (SRAM[i].key == key)  {
					SRAM[i].data = SRAM[i].data.concat(data[key][0][1]);
					break;
				}
			}
			if (i == SRAM.length) SRAM.push(new Partition(null, data[key], key));
		}
		if (++self.nShuffle < self.nTargetShuffle) return;
		self.partitions = SRAM;
		self.nextStage();
	};
}

function parallizedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	this.partitions = [];
	var partitions = param.args || [];
	for (var p = 0; p < partitions.length; p++)
		this.partitions.push(new Partition(null, partitions[p]));
}

function mappedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var mapper = recompile(param.src);

	this.transform = function(data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp[i] = mapper(data[i], param.args[0]);
		return tmp;
	}
}

function unionedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 

function Action(grid, app, job, action) {
	var stream = grid.createWriteStream(job.id, app.master_uuid);

	if (action.src) {
		action.ml = ml;
		action.src = recompile(action.src);
	}
	this.semaphore = 0;
	this.pathCount = 0;
	this.targetPathCount = 0;
	this.nextTargetPathCount = 0;
	this.sourceNodes = [];
	this.next = [];

	this.flush = function(finished) {
		if (finished) this.nextTargetPathCount--;
		if (++this.pathCount < this.targetPathCount) return;
		this.sendResult();
	}

	this.sendResult = function() {
		if ((app.wid != 0) && (++this.semaphore != 2)) return;

		// Send result to master and notify next worker to do so
		stream.write(this.result);
		if (app.worker[app.wid + 1])
			grid.send(app.worker[app.wid + 1].uuid, {cmd: 'action', jobId: job.id});

		// Reset graph nodes for next iteration
		this.reset();
		this.semaphore = 0;
		this.pathCount = 0;
		this.targetPathCount = this.nextTargetPathCount;
		this.sourceNodes = [];
		for (var n in job.node) {
			job.node[n].reset && job.node[n].reset();
			if (job.node[n].dependency == 'wide') {
				job.node[n].nShuffle = 0;
				job.node[n].targetPathCount = job.node[n].nextTargetPathCount;
				job.node[n].pathCount = 0;	
				job.node[n].sourceNodes = [];
				job.node[n].done = false;
			}
			job.node[n].closeWriteStreams();
		}

		// Unlock distant streams for next iteration
		for (var i in app.dones) app.dones[i]();
		app.dones = {};

		// Notify master about job ending (ie no active streams)
		for (var s in app.completedStreams)
			if (!app.completedStreams[s]) return;
		grid.send(app.master_uuid, {cmd: 'endJob', data: job.id});
	};
}

function Transform(grid, app, job, node) {
	for (var key in node) this[key] = node[key];	// set property to this
	var ram = {};
	var ramDirectory = '/tmp/UGRID_RAM/';
	var datasetDirectory = ramDirectory + this.id + '/' + grid.host.uuid + '/';
	var self = this;

	this.tmp = [];
	this.pathCount = 0;									// Number of finished lineages
	this.targetPathCount = 0;
	this.nextTargetPathCount = 0;
	this.sourceNodes = [];
	this.nShuffle = 0;									// Number of received shuffle (must match target to call next runSource)
	if (this.src) this.src = recompile(this.src);
	this.pipelineSemaphore = 1;							// For DiskOnlyMode
	this.running = false;

	for (var i = 0; i < this.child.length; i++) this.child[i] = job.node[this.child[i]];

	// if dataset is persistent create write streams destination directory	
	if (this.persistent) {
		try {fs.mkdirSync(ramDirectory);} catch (e) {};
		try {fs.mkdirSync(ramDirectory + this.id);} catch (e) {};
		try {fs.mkdirSync(ramDirectory + this.id + '/' + grid.host.uuid);} catch (e) {};
	}

	this.exec = function() {
		if (this.running) return;
		this.running = true;
		if (this.inMemory) {		// on connait le nombre de partitions
			this.runFromRAM();
		} else if (this.run) {		// on connait le nombre de partitions
			this.run();
		} else 
			for (var i = 0; i < this.child.length; i++) 
				this.child[i].exec();
	}

	this.closeWriteStreams = function() {
		for (var s in ram) {
			ram[s].head.end();
			ram[s].tail.end();
		}
	}

	this.saveDiskOnlyMode = function(p, array, head, done) {
		var partitionId = 0, ret;
		if (!ram[partitionId]) {
			var file = datasetDirectory + partitionId;
			ram[partitionId] = {
				head: fs.createWriteStream(file + '.pre'),
				tail: fs.createWriteStream(file)
			}
		}
		var s = head ? ram[partitionId].head : ram[partitionId].tail;
		for (var i = 0; i < array.length; i++)
			ret = s.write(JSON.stringify(array[i]) + '\n');
		if (ret) process.nextTick(done);
		else s.once('drain', done);
	};

	this.saveRamOnlyMode = function(p, array, head, done) {
		if (app.ram[this.id] == undefined) app.ram[this.id] = {};
		if (app.ram[this.id][p] == undefined) app.ram[this.id][p] = [];
		var dest = app.ram[this.id][p];

		if (head) for (var i = array.length - 1; i >= 0; i--) dest.unshift(array[i]);
		else for (var i = 0; i < array.length; i++) dest.push(array[i]);

		// immutable version
		// if (head) for (var i = array.length - 1; i > 0; i--) dest.unshift(JSON.stringify(array[i]));
		// else for (var i = 0; i < array.length; i++) dest.push(JSON.stringify(array[i]));
	};

	this.save = RAM_ONLY ? this.saveRamOnlyMode : this.saveDiskOnlyMode;

	this.pipelineToAction = function(p, head, done) {
		for (var i = 0; i < this.next.length; i++)
			this.next[i].pipeline(this.tmp, p, this.id, head, done);
	};

	this.runFromRAMRamOnlyMode = function() {
		var dataset = app.ram[this.id];
		for (var p in dataset) {
			var partition = app.ram[this.id][p];
			for (var i = 0; i < partition.length; i++) {
				// this.tmp = [JSON.parse(partition[i])];	// Immutabilité
				this.tmp = [partition[i]];
				this.pipelineToAction(p, false);
			}
		}
		this.done = true;
		this.flush(true);
	}

	this.runFromRAMDiskOnlyMode = function() {
		function readFile(path, p, end, cbk) {
			function finished() {
				if (end) {
					this.done = true;
					self.flush(true);
				}
				cbk && cbk();
			}
			if (fs.existsSync(path)) {
				var lines = new Lines2();
				fs.createReadStream(path, {encoding: 'utf8'}).pipe(lines);
				lines.on('data', function(line, done) {
					function done2() {
						if (--self.pipelineSemaphore != 0) return;
						// TODO: compute during treewalk (== number of persist on graph path between source and stageNode)
						// self.pipelineSemaphore = self.targetPipelineSemaphore;		
						self.pipelineSemaphore = 1;
						done();
					}
					self.tmp = [JSON.parse(line)];
					self.pipelineToAction(0, false, done2);
					done2();
				});
				lines.on('end', finished);
			} else finished();
		}
		// Read pre partition then partition
		var p = 0;
		var path = ramDirectory + this.id + '/' + grid.host.uuid + '/' + p + '.pre';
		readFile(path, p, false, function () {
			var p = 0;
			var path = ramDirectory + self.id + '/' + grid.host.uuid + '/' + p;
			readFile(path, p, true);
		});
	}

	this.runFromRAM = RAM_ONLY ? this.runFromRAMRamOnlyMode : this.runFromRAMDiskOnlyMode;

	this.runFromStageRam = function () {
		var input = this.SRAM || [];
		for (var p = 0; p < input.length; p++) {
			var partition = input[p].data;
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				if (this.persistent) this.save(p, this.tmp);
				this.pipelineToAction(p);
			}
		}
		// WARNING, le param true/false fixant la suppression du lineage pour l'iteration d'après dépend 
		// de la complétion des stages d'avant lorsque le noeud est un noeud de shuffle
		this.done = true;
		this.flush(true);
	}

	this.tx_shuffle = function() {
		for (var i = 0; i < this.map.length; i++)
			if (grid.host.uuid == app.worker[i].uuid) this.rx_shuffle(this.map[i]);
			else grid.send(app.worker[i].uuid, {cmd: 'shuffle', args: this.map[i], jobId: job.id, shuffleNode: this.num});		
	};

	////// ATTENTION CI-DESSOUS IL FAUT FIXER la gestion du nombre de targetPathCount
	this.flush = function(finished) {
		if ((this.dependency == 'narrow') || this.done) {
			for (var i = 0; i < this.next.length; i++) this.next[i].flush(finished);
		} else {
			if (finished) this.nextTargetPathCount--;
			if (++this.pathCount == this.targetPathCount) {
				this.tx_shuffle();
				if (this.nShuffle == app.worker.length) this.runFromStageRam();
			}
		}
	}
}

// ------------------------------------------------------------------------------------ //
// Actions
// ------------------------------------------------------------------------------------ //
this.takeOrdered = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	var num = action.args[0];
	var sorter = action.src;
	this.result = [];

	this.reset = function() {
		this.result = [];
	};

	this.pipeline = function(array) {
		this.result = this.result.concat(array).sort(sorter).slice(0, num);
	};
};

this.top = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	var num = action.args[0];
	var sorter = action.src;
	this.result = [];

	this.reset = function() {
		this.result = [];
	};

	this.pipeline = function(array) {
		this.result = this.result.concat(array).sort(sorter).slice(0, num);
	};
};

this.take = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = [];
	var num = action.args[0];
	var self = this;

	this.reset = function() {
		this.result = [];
	};

	this.pipeline = function(array) {
		if (this.result.length < num)
			this.result = this.result.concat(array).slice(0, num);
	};
};

this.count = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = 0;

	this.reset = function() {
		this.result = 0;
	};

	this.pipeline = function(array) {
		this.result += array.length;
	};
};

this.lookup = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = {};
	var key = action.args[0];

	this.reset = function() {
		this.result = {};
	};

	this.pipeline = function (array, p) {
		if (this.result[p] === undefined) this.result[p] = [];
		var dest = this.result[p];
		for (var i = 0; i < array.length; i++)
			if (array[i][0] == key) dest.push(array[i]);
	};
};

this.countByValue = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = {};

	this.reset = function() {
		this.result = {};
	};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			if (this.result[str] === undefined)
				this.result[str] = [array[i], 0];
			this.result[str][1]++;
		}
	};
};

this.forEach = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	var each = action.src;

	this.reset = function() {;};

	this.pipeline = function(array) {
		array.forEach(each);
	};
};

// ------------------------------------------------------------------------------------ //
// Sources
// ------------------------------------------------------------------------------------ //
this.stream = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
 	var self = this;
 	var n = 0;
 	var N = this.args[0];
 	var streamIdx = this.args[1];

	app.completedStreams[streamIdx] = false;

	var onData = function(data, done) {			
		self.tmp = [data];
		if (self.persistent) self.save(0, self.tmp);
		self.pipelineToAction(0);
		if (++n == N) {
			n = 0;
			app.dones[streamIdx] = done;
			self.flush(false);
		} else done();
	}

	var onBlock = function(done) {
		app.dones[streamIdx] = done;
		self.flush(false);
	}

	var onEnd = function(done) {
		grid.removeListener(streamIdx, onData);
		grid.removeListener(streamIdx + '.block', onBlock);
		grid.removeListener(streamIdx + '.end', onEnd);
		app.completedStreams[streamIdx] = true;
		app.dones[streamIdx] = done;
		self.flush(true);
	}

	grid.on(streamIdx, onData);
	grid.on(streamIdx + '.block', onBlock);
	grid.on(streamIdx + '.end', onEnd);

	this.run = function () {;};
};

// ------------------------------------------------------------------------------------ //
// Transformation
// ------------------------------------------------------------------------------------ //
this.filter = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var filter = this.src;

	this.pipeline = function(array, p, from_id, head, done) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			if (filter(array[i])) tmp.push(array[i]);
		if (this.persistent) this.save(p, tmp, head, done);
		for (var i = 0; i < this.next.length; i++)
			this.next[i].pipeline(tmp, p, this.id, head, done);
	};
};

this.flatMap = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var mapper = this.src;

	this.pipeline = function(array, p, from_id, head, done) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			tmp = tmp.concat(mapper(array[i]));
		if (this.persistent) this.save(p, tmp, head, done);
		for (var i = 0; i < this.next.length; i++)
			this.next[i].pipeline(tmp, p, this.id, head, done);
	};
};

this.mapValues = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var mapper = this.src;

	this.pipeline = function(array, p, from_id, head, done) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			tmp[i] = [array[i][0], mapper(array[i][1])];
		if (this.persistent) this.save(p, tmp, head, done);
		for (var i = 0; i < this.next.length; i++)
			this.next[i].pipeline(tmp, p, this.id, head, done);
	};
};

this.flatMapValues = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var mapper = this.src;

	this.pipeline = function(array, p, from_id, head, done) {
		var tmp = [];
		for (var i = 0; i < array.length; i++) {
			var t0 = mapper(array[i][1]);
			tmp = tmp.concat(t0.map(function(e) {return [array[i][0], e];}));
		}
		if (this.persistent) this.save(p, tmp, head, done);
		for (var i = 0; i < this.next.length; i++)
			this.next[i].pipeline(tmp, p, this.id, head, done);
	};
};

this.distinct = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var tmp, map;

	this.reset = function() {
		this.SRAM = [];
		tmp = [];
		map = this.map = app.worker.map(function() {return [];});
	};

	this.reset();

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % app.worker.length;
			if (map[wid].indexOf(str) == -1) map[wid].push(str);
		}
	};

	this.rx_shuffle = function (data) {
		for (var i = 0; i < data.length; i++)
			if (tmp.indexOf(data[i]) == -1) tmp.push(data[i]);
		if (++this.nShuffle < app.worker.length) return;
		this.SRAM = [{data: tmp.map(JSON.parse)}];
	};
};

this.coGroup = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var SRAM, map;

	this.reset = function() {
		SRAM = this.SRAM = {};
		map = this.map = app.worker.map(function() {return {};});
	};

	this.reset();

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined) map[wid][key] = {key: key};
			if (map[wid][key][from_id] === undefined) map[wid][key][from_id] = [];
			map[wid][key][from_id].push(array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		var i, k;
		for (i in data)
			if (i in SRAM) {
				for (k in data[i]) {
					if (k == 'key') continue;
					if (SRAM[i][k] === undefined) SRAM[i][k] = data[i][k];
					else SRAM[i][k] = SRAM[i][k].concat(data[i][k]);
				}
			} else SRAM[i] = data[i];
		if (++this.nShuffle < app.worker.length) return;
		var res = [];
		for (i in SRAM) {
			var datasets = Object.keys(SRAM[i]);
			if (datasets.length != 3) continue;
			res.push({
				key: SRAM[i].key,
				data:[[SRAM[i].key, [SRAM[i][this.child[0].id], SRAM[i][this.child[1].id]]]]
			});
		}
		this.SRAM = res;
	};
};

this.sample = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var withReplacement = this.args[0];
	var frac = this.args[1];
	var seed = this.args[2];
	var tmp;

	this.reset = function() {
		this.SRAM = [];
		tmp = {};
	};

	this.reset();

	this.pipeline = function(array, p) {
		if (tmp[p] === undefined) tmp[p] = [];
		for (var i = 0; i < array.length; i++)
			tmp[p].push(array[i]);
	};

	this.tx_shuffle = function() {
		var p = 0;
		var rng = new ml.Random(seed);
		for (var i in tmp) {
			var L = Math.ceil(tmp[i].length * frac);
			var data = [];
			var idxVect = [];
			while (data.length != L) {
				var idx = Math.round(Math.abs(rng.next()) * (L - 1));
				if (!withReplacement && (idxVect.indexOf(idx) != -1))
					continue;	// if already picked but no replacement mode
				idxVect.push(idx);
				data.push(tmp[i][idx]);
			}
			this.SRAM[p++] = {data: data};
		}
		this.nShuffle = app.worker.length;	// No shuffle
	};
};

this.join = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var type = this.args[1];
	var tmp, map;

	this.reset = function() {
		this.SRAM = [];
		tmp = {};
		tmp[this.child[0].id] = {};
		tmp[this.child[1].id] = {};
		map = this.map = app.worker.map(function() {return {};});
		for (var i = 0; i < app.worker.length; i++) {
			map[i][this.child[0].id] = {};
			map[i][this.child[1].id] = {};
		}
	};

	this.reset();

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var str = JSON.stringify(key);
			var wid = ml.cksum(str) % app.worker.length;
			if (map[wid][from_id][key] === undefined)
				map[wid][from_id][key] = {key: key, data: []};
			map[wid][from_id][key].data.push(array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		var i, j, k, dataset;
		for (dataset in data) {
			if (tmp[dataset] === undefined) {
				tmp[dataset] = data[dataset];
				continue;
			}
			for (var key in data[dataset]) {
				if (tmp[dataset][key] === undefined) {
					tmp[dataset][key] = data[dataset][key];
					continue;
				}
				tmp[dataset][key].data = tmp[dataset][key].data.concat(data[dataset][key].data);
			}
		}
		if (++this.nShuffle < app.worker.length) return;

		var key0 = Object.keys(tmp[this.child[0].id]);
		var key1 = Object.keys(tmp[this.child[1].id]);

		switch (type) {
		case 'inner':
			for (i = 0; i < key0.length; i++) {
				var key = tmp[this.child[0].id][key0[i]].key;
				if (key1.indexOf(key0[i]) == -1) continue;
				var values = [tmp[this.child[0].id][key0[i]].data, tmp[this.child[1].id][key0[i]].data];				
				var tmp_data = [];
				for (j = 0; j < values[0].length; j++)
					for (k = 0; k < values[1].length; k++) {
						tmp_data.push([key, [values[0][j], values[1][k]]]);
				}
				this.SRAM.push({key: key, data: tmp_data});
			}
			break;
		case 'left':
			for (i = 0; i < key0.length; i++) {
				var key = tmp[this.child[0].id][key0[i]].key;
				if (key1.indexOf(key0[i]) == -1) {
					var values = tmp[this.child[0].id][key0[i]].data;
					data = [];
					for (j = 0; j < values.length; j++)
						data.push([key, [values[j], null]]);
				} else {
					var values = [tmp[this.child[0].id][key0[i]].data, tmp[this.child[1].id][key0[i]].data];
					data = [];
					for (j = 0; j < values[0].length; j++)
						for (k = 0; k < values[1].length; k++)
						data.push([key, [values[0][j], values[1][k]]]);						
				}
				this.SRAM.push({key: key, data: data});
			}
			break;
		case 'right':
			for (i = 0; i < key1.length; i++) {
				var key = tmp[this.child[1].id][key1[i]].key;
				if (key0.indexOf(key1[i]) == -1) {
					var values = tmp[this.child[1].id][key1[i]].data;
					data = [];
					for (j = 0; j < values.length; j++)
						data.push([key, [null, values[j]]]);
				} else {
					var values = [tmp[this.child[0].id][key1[i]].data, tmp[this.child[1].id][key1[i]].data];					
					data = [];
					for (j = 0; j < values[0].length; j++)
						for (k = 0; k < values[1].length; k++)
							data.push([key, [values[0][j], values[1][k]]]);
				}
				this.SRAM.push({key: key, data: data});
			}
			break;
		}
	};
};


this.crossProduct = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var left_dataset = this.child[0].id;
	var residentPartitions, shuffledPartitions, map, rxPartitions;

	this.reset = function() {
		this.SRAM = [];
		residentPartitions = [];
		shuffledPartitions = [];
		map = this.map = app.worker.map(function() {return shuffledPartitions;});
		rxPartitions = [];
	};

	this.reset();

	this.pipeline = function(array, p, from_id) {
		var dest = (from_id == left_dataset) ? residentPartitions : shuffledPartitions;
		if (dest[p] === undefined) dest[p] = [];
		for (var i = 0; i < array.length; i++) dest[p].push(array[i]);
	};

	function crossProduct(a, b) {
		var t0 = [];
		for (var i = 0; i < a.length; i++)
			for (var j = 0; j < b.length; j++)
				t0.push([a[i], b[j]]);
		return t0;
	}

	this.rx_shuffle = function(data) {
		var i, j;
		for (i = 0; i < data.length; i++)
			rxPartitions.push(data[i]);

		if (++this.nShuffle < app.worker.length) return;
		for (j = 0; j < rxPartitions.length; j++) {
			for (i = 0; i < residentPartitions.length; i++)
				this.SRAM.push({data: crossProduct(residentPartitions[i], rxPartitions[j])});
		}
	};
};

this.intersection = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var tmp, map;

	this.reset = function() {
		this.SRAM = [];
		tmp = {};
		tmp[this.child[0].id] = [];
		tmp[this.child[1].id] = [];
		map = this.map = app.worker.map(function() {return {};});
		for (var i = 0; i < app.worker.length; i++) {
			map[i][this.child[0].id] = [];
			map[i][this.child[1].id] = [];
		}
	};

	this.reset();

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % app.worker.length;
			map[wid][from_id].push(str);
		}
	};

	this.rx_shuffle = function(data) {
		var i, j, k;
		for (i in data) tmp[i].push(data[i]);
		if (++this.nShuffle < app.worker.length) return;
		var result = [];
		for (i = 0; i < tmp[this.child[0].id].length; i++)
			loop:
			for (j = 0; j < tmp[this.child[0].id][i].length; j++) {
				var e = tmp[this.child[0].id][i][j];
				if (result.indexOf(e) != -1) continue;
				for (k = 0; k < tmp[this.child[1].id].length; k++)
					if (tmp[this.child[1].id][k].indexOf(e) != -1) {
						result.push(e);
						continue loop;
					}
			}
		this.SRAM = [{data: []}];
		for (i = 0; i < result.length; i++)
			this.SRAM[0].data.push(JSON.parse(result[i]));
	};
};

this.subtract = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var tmp, map;

	this.reset = function() {
		this.SRAM = [];
		tmp = {};
		tmp[this.child[0].id] = [];
		tmp[this.child[1].id] = [];
		map = this.map = app.worker.map(function() {return {};});
		for (var i = 0; i < app.worker.length; i++) {
			map[i][this.child[0].id] = [];
			map[i][this.child[1].id] = [];
		}
	};

	this.reset();

	this.pipeline = function(array, p, from_id) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i]);
			var wid = ml.cksum(str) % app.worker.length;
			map[wid][from_id].push(str);
		}
	};

	this.rx_shuffle = function(data) {
		var i;
		for (i in data) tmp[i] = tmp[i].concat(data[i]);
		if (++this.nShuffle < app.worker.length) return;
		var v1 = tmp[this.child[0].id];
		var v2 = tmp[this.child[1].id];
		var v_ref = [];
		for (i = 0; i < v1.length; i++) {
			//if (v_ref.indexOf(v1[i]) != -1) continue;
			if (v2.indexOf(v1[i]) != -1) continue;
			v_ref.push(v1[i]);
		}
		this.SRAM = [{data: v_ref.map(JSON.parse)}];
	};
};

this.partitionByKey = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var tmp, map;

	this.reset = function() {
		this.SRAM = [];
		tmp = {};
		map = this.map = app.worker.map(function() {return {};});
	};

	this.reset();

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var str = JSON.stringify(array[i][0]);
			var wid = ml.cksum(str) % app.worker.length;
			if (map[wid][str] === undefined)
				map[wid][str] = [];
			map[wid][str].push(array[i]);
		}
	};

	this.rx_shuffle = function (data) {
		var key;
		for (key in data) {
			if (tmp[key] === undefined) tmp[key] = data[key];
			else tmp[key] = tmp[key].concat(data[key]);
		}
		if (++this.nShuffle < app.worker.length) return;
		for (key in tmp)
			this.SRAM.push({data: tmp[key], key: JSON.parse(key)});
	};
};

this.sortByKey = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var tmp, tmp2, keys, map;

	this.reset = function() {
		this.SRAM = [];
		tmp = {};
		tmp2 = {};
		keys = [];
		map = this.map = app.worker.map(function() {return tmp;});
	};

	this.reset();

	function split(a, n) {
		var len = a.length, out = [], i = 0;
		while (i < len) {
			var size = Math.ceil((len - i) / n--);
			out.push(a.slice(i, i += size));
		}
		return out;
	}

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			if (tmp[array[i][0]] === undefined)
				tmp[array[i][0]] = {key: array[i][0], data: []};
			tmp[array[i][0]].data.push(array[i][1]);
		}
	};

	this.rx_shuffle = function (data) {
		for (var key in data) {
			if (tmp2[key] === undefined) {
				tmp2[key] = data[key];
				keys.push(key);
				keys.sort();
			} else tmp2[key] = tmp2[key].concat(data[key]);
		}
		if (++this.nShuffle < app.worker.length) return;
		// Compute partition mapping over workers
		var mapping = split(keys, app.worker.length);
		for (var i = 0; i < mapping.length; i++) {
			if (app.worker[i].uuid != grid.host.uuid) continue;
			for (var j = 0; j < mapping[i].length; j++)
				this.SRAM.push({key: tmp2[mapping[i][j]].key, data: tmp2[mapping[i][j]].data.map(function(e) {
					return [tmp2[mapping[i][j]].key, e];
				})});
		}
	};
};
