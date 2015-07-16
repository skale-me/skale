'use strict';

var fs = require('fs');

var ml = require('./ugrid-ml.js');
var trace = require('line-trace');
var Lines = require('./lines.js');
var Lines2 = require('./lines2.js');

var RAM_ONLY = true;

function recompile(s) {
	var args = s.match(/\(([^)]*)/)[1];
	var body = s.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
	return new Function(args, body);
}

var self = this;

module.exports.UgridJob = function(grid, app, param) {
	this.id = param.jobId;
	this.node = param.node;
	this.app = app;
	this.action = new self[param.action.fun](grid, app, this, param.action);

	var nums = Object.keys(param.node).sort(function(a, b){return b - a});
	for (var i = 0; i < nums.length; i++)
		this.node[nums[i]] = new self[this.node[nums[i]].type](grid, app, this, param.node[nums[i]]);

	this.findSource = function(from, n) {
		if (from) n.next.push(from);
		else n.next.push(this.action);
		for (var i = 0; i < n.child.length; i++) 
			this.findSource(n, n.child[i]);
	}
	this.findSource(null, this.node[nums[nums.length - 1]]);

	this.run = function() {
		this.node[nums[nums.length - 1]].exec();
	}	
};

function Action(grid, app, job, action) {
	var stream = grid.createWriteStream(job.id, app.master_uuid);

	if (action.src) {
		action.ml = ml;
		action.src = recompile(action.src);
	}
	this.semaphore = 0;
	this.pathCount = 0;
	this.targetPathCount = action.targetPathCount;
	this.nextTargetPathCount = this.targetPathCount;
	this.next = [];

	// CHANGER l'interface du pipeline de l'action pour repousser le head en dernier arg, après l'id du caller

	this.lineageEnd = function(finished) {
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
		for (var n in job.node) {
			job.node[n].reset && job.node[n].reset();
			if (job.node[n].dependency == 'wide') {
				job.node[n].nShuffle = 0;
				job.node[n].targetPathCount = job.node[n].nextTargetPathCount;
				job.node[n].pathCount = 0;
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

	this.inLastStage = (this.stageNode == undefined);
	this.nodePath = [this];
	this.tmp = [];
	this.pathCount = 0;									// Number of finished lineages
	this.nextTargetPathCount = this.targetPathCount;	// next iteration lineage target count
	this.nShuffle = 0;									// Number of received shuffle (must match target to call next runSource)
	if (this.src) this.src = recompile(this.src);
	this.pipelineSemaphore = 1;							// For DiskOnlyMode

	for (var i = 0; i < this.child.length; i++)
		this.child[i] = job.node[this.child[i]];

	// if dataset is persistent create write streams destination directory	
	if (this.persistent) {
		try {fs.mkdirSync(ramDirectory);} catch (e) {};
		try {fs.mkdirSync(ramDirectory + this.id);} catch (e) {};
		try {fs.mkdirSync(ramDirectory + this.id + '/' + grid.host.uuid);} catch (e) {};
	}

	this.exec = function() {
		if (this.inMemory) this.runFromRAM();
		else if (this.run) this.run();
		else {
			for (var i = 0; i < this.child.length; i++) 
				this.child[i].exec();
		}
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
		else s.once('drain', done)
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
		var tmp = this;
		while (tmp.next.length) {
			this.tmp = tmp.next[0].pipeline(this.tmp, p, tmp.id);
			if (tmp.next[0].persistent && (tmp.next[0].dependency == 'narrow'))
				tmp.next[0].save(p, this.tmp, head, done);
			if (tmp.next[0].dependency == 'wide') break;
			tmp = tmp.next[0];
		}
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
		if (this.stageNode) job.node[this.stageNode].lineageEnd(true);
		else job.action.lineageEnd(true);
	}

	this.runFromRAMDiskOnlyMode = function() {
		function readFile(path, p, end, cbk) {
			function finished() {
				if (end) {
					if (self.stageNode) job.node[self.stageNode].lineageEnd(true);
					else job.action.lineageEnd(true);
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
		if (this.stageNode) job.node[this.stageNode].lineageEnd(true);
		else job.action.lineageEnd(true);
	}

	this.tx_shuffle = function() {
		for (var i = 0; i < this.map.length; i++)
			if (grid.host.uuid == app.worker[i].uuid) this.rx_shuffle(this.map[i]);
			else grid.send(app.worker[i].uuid, {cmd: 'shuffle', args: this.map[i], jobId: job.id, shuffleNode: this.num});		
	};

	this.lineageEnd = function(finished) {
		if (finished) this.nextTargetPathCount--;
		if (++this.pathCount < this.targetPathCount) return;
		this.tx_shuffle();
		if (this.nShuffle == app.worker.length) this.runFromStageRam();
	}
}

this.randomSVMData = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var D = this.args[0];
	var partition = this.args[1] || [];

	var p = 0, i = 0, self = this;
	var rng = new ml.Random(partition[p].seed);

	this.runRamOnlyMode = function() {
		for (var p = 0; p < partition.length; p++) {
			var rng = new ml.Random(partition[p].seed);
			for (var i = 0; i < partition[p].n; i++) {
				this.tmp = [ml.randomSVMLine(rng, D)];
				if (this.persistent) this.save(p, this.tmp);
				this.pipelineToAction(p);
			}
		}
		if (this.stageNode) job.node[this.stageNode].lineageEnd(true);
		else job.action.lineageEnd(true);		
	}

	// Experiment for DiskOnlyMode
	this.targetPipelineSemaphore = 2;			// Must be computed during treewalk
	this.runDiskOnlyMode = function() {
		if (--self.pipelineSemaphore != 0) return;
		self.pipelineSemaphore = self.targetPipelineSemaphore;

		if (++i > partition[p].n) {
			i = 0;
			p = (p + 1) % partition.length;
			if (p == 0) {
				if (self.stageNode) job.node[self.stageNode].lineageEnd(true);
				else job.action.lineageEnd(true);
				return;
			}
		}
		self.tmp = [ml.randomSVMLine(rng, D)];
		if (self.persistent) self.save(p, self.tmp, false, self.run);
		self.pipelineToAction(p, false, self.run);
		process.nextTick(self.run);
	}

	// this.run = this.runRamOnlyMode;
	this.run = RAM_ONLY ? this.runRamOnlyMode : this.runDiskOnlyMode;	
}

this.collect = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = {};

	this.reset = function() {
		this.result = {};
	};

	this.pipeline = function(array, p, from_id, head) {
		if (this.result[p] === undefined) this.result[p] = [];
		var i, dest = this.result[p];
		if (head)
			for (i = array.length - 1; i >= 0; i--) dest.unshift(array[i]);
		else
			for (i = 0; i < array.length; i++) dest.push(array[i]);
	};
};

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

this.reduce = function(grid, app, job, action) {
	Action.call(this, grid, app, job, action);
	this.result = JSON.parse(JSON.stringify(action.args[0]));
	var reduce = action.src;

	this.reset = function() {
		this.result = JSON.parse(JSON.stringify(action.args[0]));
	};

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++)
			this.result = reduce(this.result, array[i]);
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
// Transformations
// ------------------------------------------------------------------------------------ //
this.parallelize = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);

	this.run = function() {
		var input = this.args[0] || [];
		for (var p = 0; p < input.length; p++) {
			var partition = input[p];
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				if (this.persistent) this.save(p, this.tmp);
				this.pipelineToAction(p);
			}
		}
		if (this.stageNode) job.node[this.stageNode].lineageEnd(true);
		else job.action.lineageEnd(true);
	};
}

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
			if (self.stageNode) job.node[self.stageNode].lineageEnd(false);
			else job.action.lineageEnd(false);
		} else done();
	}

	var onBlock = function(done) {
		app.dones[streamIdx] = done;
		if (self.stageNode) job.node[self.stageNode].lineageEnd(false);
		else job.action.lineageEnd(false);
	}

	var onEnd = function(done) {
		grid.removeListener(streamIdx, onData);
		grid.removeListener(streamIdx + '.block', onBlock);
		grid.removeListener(streamIdx + '.end', onEnd);
		app.completedStreams[streamIdx] = true;
		app.dones[streamIdx] = done;
		if (self.stageNode) job.node[self.stageNode].lineageEnd(true);
		else job.action.lineageEnd(true);
	}

	grid.on(streamIdx, onData);
	grid.on(streamIdx + '.block', onBlock);
	grid.on(streamIdx + '.end', onEnd);

	this.run = function () {;};
};

this.textFile = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var self = this;
 	var blocks = this.args[2];
	var hashedBlocks = {};
	var blockIdx = 0;

	for (var i = 0; i < blocks.length; i++) {
		blocks[i].p = i;
		hashedBlocks[blocks[i].bid] = blocks[i];
	}

	this.run = function() {
		if (blocks.length === 0) {
			if (this.stageNode) job.node[this.stageNode].lineageEnd(true);
			else job.action.lineageEnd(true);
		} else processBlock(blockIdx);
	};

	function processLine(p, line, first) {
		self.tmp = [line];
		if (self.persistent) self.save(p, self.tmp, first);
		self.pipelineToAction(p, first);
	}

	function processBlock(bid) {
		var lines = new Lines();
		fs.createReadStream(blocks[bid].file, blocks[bid].opt).pipe(lines);

		lines.once("data", function (line) {
			blocks[bid].firstLine = line;
			lines.once("data", function (line) {
				blocks[bid].lastLine = line;
				lines.on("data", function (line) {
					self.tmp = [blocks[bid].lastLine];
					if (self.persistent) self.save(blocks[bid].p, self.tmp);
					self.pipelineToAction(blocks[bid].p);
					blocks[bid].lastLine = line;
				});
			});
		});

		function shuffleLine(shuffledLine) {
			var shuffleTo = blocks[bid].shuffleTo;
			if (shuffleTo != app.wid) {
				grid.send(app.worker[shuffleTo].uuid, {
					cmd: 'lastLine', 
					args: {lastLine: shuffledLine, targetNum: self.num, bid: blocks[bid].bid + 1}, 
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
			if (isFirstBlock) {	// First block
				firstLineProcessed = true;
				if (hasLastLine || lastLineComplete || isLastBlock)
					processLine(blocks[bid].p, blocks[bid].firstLine, true);
				if (!hasLastLine && !lastLineComplete && !isLastBlock)
					shuffleLine(blocks[bid].firstLine);
			} else {
				blocks[bid].forward = (!hasLastLine && !lastLineComplete) ? true : false;
				firstLineProcessed = processFirstLine(blocks[bid].bid);
			}
			// LAST LINE
			if (hasLastLine) {
				if (isLastBlock) processLine(blocks[bid].p, blocks[bid].lastLine);
				else if (lastLineComplete) {
					processLine(blocks[bid].p, blocks[bid].lastLine);
					shuffleLine('');
				} else shuffleLine(blocks[bid].lastLine);
			} else if (lastLineComplete && !isLastBlock)
				shuffleLine('');

			if (!firstLineProcessed) return;
			if ((blockIdx + 1) < blocks.length) {
				processBlock(++blockIdx);
			} else {
				if (self.stageNode) job.node[self.stageNode].lineageEnd(true);
				else job.action.lineageEnd(true);
			}
		});
	}

	var processLastLine = this.processLastLine = function(data) {
		var targetBlock = hashedBlocks[data.bid];
		targetBlock.rxLastLine = data.lastLine;
		targetBlock.hasReiceivedLastLine = true;
		var firstLineProcessed = processFirstLine(data.bid);

		if (!firstLineProcessed) return;
		if ((blockIdx + 1) < blocks.length) {
			processBlock(++blockIdx);
		} else {
			if (self.stageNode) job.node[self.stageNode].lineageEnd(true);
			else job.action.lineageEnd(true);
		}
	};

	function processFirstLine(bid) {
		var targetBlock = hashedBlocks[bid];
		if (!targetBlock.hasReiceivedLastLine || !targetBlock.hasScannedFile) return false;
		if (targetBlock.forward && targetBlock.shuffleLastLine) {
			var shuffledLine = targetBlock.rxLastLine + targetBlock.firstLine;
			if (targetBlock.shuffleTo != app.wid) {
				grid.send(app.worker[targetBlock.shuffleTo].uuid, {
					cmd: 'lastLine', 
					args: {lastLine: shuffledLine, targetNum: self.num, bid: bid + 1}
				});
			} else processLastLine({lastLine: shuffledLine, bid: bid + 1});
		} else {
			var str = (targetBlock.rxLastLine === undefined) ? targetBlock.firstLine : targetBlock.rxLastLine + targetBlock.firstLine;
			processLine(targetBlock.p, str, true);
		}
		return true;
	}
}

this.union = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	this.pipeline = function (array) {return array;};
};

this.map = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var mapper = this.src;
	var args = this.args;

	this.pipeline = function(array) {
		var tmp = []
		for (var i = 0; i < array.length; i++)
			tmp[i] = mapper(array[i], args[0]);
		return tmp;
	};	
};

this.filter = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var filter = this.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			if (filter(array[i])) tmp.push(array[i]);
		return tmp;
	};
};

this.flatMap = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var mapper = this.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			tmp = tmp.concat(mapper(array[i]));
		return tmp;
	};
};

this.mapValues = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var mapper = this.src;

	// this.pipeline = function(array) {
	// 	for (var i = 0; i < array.length; i++)
	// 		array[i][1] = mapper(array[i][1]);
	// 	return array;
	// };
	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++)
			tmp[i] = [array[i][0], mapper(array[i][1])];
			// array[i][1] = mapper(array[i][1]);
		return tmp;
	};	
};

this.flatMapValues = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var mapper = this.src;

	this.pipeline = function(array) {
		var tmp = [];
		for (var i = 0; i < array.length; i++) {
			var t0 = mapper(array[i][1]);
			tmp = tmp.concat(t0.map(function(e) {return [array[i][0], e];}));
		}
		return tmp;
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

this.reduceByKey = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var reducer = this.src;
	var initVal = this.args[0];
	var SRAM, map;

	this.reset = function() {
		SRAM = this.SRAM = [];
		map = this.map = app.worker.map(function() {return {};});
	};

	this.reset();

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined)
				map[wid][key] = [[key, JSON.parse(JSON.stringify(initVal))]];
			map[wid][key][0][1] = reducer(map[wid][key][0][1], array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++)
				if (SRAM[i].key == key) break;
			if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
			else SRAM[i].data[0][1] = reducer(SRAM[i].data[0][1], data[key][0][1]);
		}
		this.nShuffle++;
	};
};

this.groupByKey = function(grid, app, job, node) {
	Transform.call(this, grid, app, job, node);
	var SRAM, map;

	this.reset = function() {
		SRAM = this.SRAM = [];
		map = this.map = app.worker.map(function() {return {};});
	};

	this.reset();

	this.pipeline = function(array) {
		for (var i = 0; i < array.length; i++) {
			var key = array[i][0];
			var wid = ml.cksum(key) % app.worker.length;
			if (map[wid][key] === undefined) map[wid][key] = [[key, []]];
			map[wid][key][0][1].push(array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		// console.log(data)
		for (var key in data) {
			for (var i = 0; i < SRAM.length; i++)
				if (SRAM[i].key == key) break;
			if (i == SRAM.length) SRAM.push({key: key, data: data[key]});
			else SRAM[i].data[0][1] = SRAM[i].data[0][1].concat(data[key][0][1]);
		}
		this.nShuffle++;
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
				map[wid][from_id][key] = [];
			map[wid][from_id][key].push(array[i][1]);
		}
	};

	this.rx_shuffle = function(data) {
		var i, j, k;
		for (i in data) {
			if (tmp[i] === undefined) {
				tmp[i] = data[i];
				continue;
			}
			for (var key in data[i]) {
				if (tmp[i][key] === undefined) {
					tmp[i][key] = data[i][key];
					continue;
				}
				tmp[i][key] = tmp[i][key].concat(data[i][key]);
			}
		}
		if (++this.nShuffle < app.worker.length) return;
		var key0 = Object.keys(tmp[this.child[0].id]);
		var key1 = Object.keys(tmp[this.child[1].id]);

		switch (type) {
		case 'inner':
			for (i = 0; i < key0.length; i++) {
				if (key1.indexOf(key0[i]) == -1) continue;
				data = [];
				for (j = 0; j < tmp[this.child[0].id][key0[i]].length; j++)
					for (k = 0; k < tmp[this.child[1].id][key0[i]].length; k++)
						data.push([JSON.parse(key0[i]), [tmp[this.child[0].id][key0[i]][j], tmp[this.child[1].id][key0[i]][k]]]);
				this.SRAM.push({key: key0[i], data: data});
			}
			break;
		case 'left':
			for (i = 0; i < key0.length; i++) {
				if (key1.indexOf(key0[i]) == -1) {
					data = [];
					for (j = 0; j < tmp[this.child[0].id][key0[i]].length; j++)
						data.push([JSON.parse(key0[i]), [tmp[this.child[0].id][key0[i]][j], null]]);
				} else {
					data = [];
					for (j = 0; j < tmp[this.child[0].id][key0[i]].length; j++)
						for (k = 0; k < tmp[this.child[1].id][key0[i]].length; k++)
							data.push([JSON.parse(key0[i]), [tmp[this.child[0].id][key0[i]][j], tmp[this.child[1].id][key0[i]][k]]]);
				}
				this.SRAM.push({key: key0[i], data: data});
			}
			break;
		case 'right':
			for (i = 0; i < key1.length; i++) {
				if (key0.indexOf(key1[i]) == -1) {
					data = [];
					for (j = 0; j < tmp[this.child[1].id][key1[i]].length; j++)
						data.push([JSON.parse(key1[i]), [null, tmp[this.child[1].id][key1[i]][j]]]);
				} else {
					data = [];
					for (j = 0; j < tmp[this.child[0].id][key1[i]].length; j++)
						for (k = 0; k < tmp[this.child[1].id][key1[i]].length; k++)
							data.push([JSON.parse(key1[i]), [tmp[this.child[0].id][key1[i]][j], tmp[this.child[1].id][key1[i]][k]]]);
				}
				this.SRAM.push({key: key1[i], data: data});
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
