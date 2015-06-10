'use strict';

var fs = require('fs');
var RDD = require('./ugrid-transformation.js');
var trace = require('line-trace');
var ml = require('./ugrid-ml.js');
var Lines = require('./lines.js');

module.exports.UgridJob = function(grid, app, param) {
	var nStage = param.stageData.length;

	this.id = param.jobId;
	this.node = param.node;
	this.stage = [];
	this.scnt = 0;
	this.app = app;
	this.action = new RDD[param.actionData.fun](grid, app, this, param.actionData);

	for (var i = 0; i < nStage; i++)
		this.stage[i] = new Stage(grid, app, this, {
			sid: i, 
			stageData: param.stageData[i], 
			isLastStage: i == (nStage - 1)
		});

	this.run = function() {this.stage[0].run();};
};

function Stage(grid, app, job, param) {
	var node = job.node;
	var lineages = param.stageData.lineages;
	var shuffleNum = param.stageData.shuffleNum;
	var self = this;

	this.source = [];
	this.cnt = 0;									// Number of finished lineages
	this.target_cnt = lineages.length;				// Number of lineages
	this.locked = false;							// Because of an asynchonous lineage
	this.nShuffle = 0;								// Number of shuffle received
	this.sid = param.sid;							// stage index
	this.next_target_cnt = lineages.length;			// next iteration lineage target count
	this.shuffleNum = param.stageData.shuffleNum;	// shuffle node number

	for (var l = 0; l < lineages.length; l++) {
		for (var j = 1; j < lineages[l].length; j++) {
			var n = node[lineages[l][j]];
			if (typeof(n.transform) == 'string')
				n.transform = new RDD[n.transform](grid, app, job, this, n);
		}
		this.source[l] = new UgridSource[node[lineages[l][0]].type](grid, app, job, this, {
			lid: l, 
			transform: lineages[l], 
			inLastStage: param.isLastStage
		});
	}

	this.run = function () {
		for (var l = 0; l < this.source.length; l++) {
			try {
				this.source[l].run(function(ignore) {
					if ((++self.cnt < self.target_cnt) || self.locked) return;
					if (ignore || param.isLastStage) {
						// // if ignore (WARNING: only for one source stream) run next shuffle to prevent stage lock
						// // and execute immediatly action.sendResult();
						// for (var i = job.scnt + 1; i < job.stage.length; i++)
						// 	job.node[job.stage[i].shuffleNum].transform.tx_shuffle();
						job.action.sendResult(ignore);
					} else {
						node[shuffleNum].transform.tx_shuffle();
						if (self.nShuffle == app.worker.length)
							job.stage[++job.scnt].run();
					}
				});
			} catch (err) {
				console.error(err.stack);
				throw new Error("Lineage error, " + err);
			}
		}
	};
}

function Source(grid, app, job, stage, param) {
	var node = job.node;
	var RAM = app.RAM;
	var RAM_DIR = '/tmp/UGRID_RAM/';
	var partitionMapper = {};
	var partitionMapperToFS = {};
	this.tmp = [];
	var transform = param.transform;

	this.save = function(t, head) {
		var i, id = node[transform[t]].id;
		if (partitionMapper[id] === undefined) {				// le dataset id n'existe pas, on le crée
			partitionMapper[id] = [];							// Nouveau vecteur associé au lineage
			partitionMapper[id][t] = 0;
			RAM[id] = [{data: []}];								// nouveau vecteur de partition dans la RAM
			partitionMapperToFS[id] = [];
			try {fs.mkdirSync(RAM_DIR);} catch (e) {};
			try {fs.mkdirSync(RAM_DIR + id);} catch (e) {};
			try {fs.mkdirSync(RAM_DIR + id + '/' + grid.host.uuid);} catch (e) {};
			partitionMapperToFS[id][t] = fs.createWriteStream('/tmp/UGRID_RAM/' + id + '/' + grid.host.uuid + '/0');
		} else if (partitionMapper[id][t] === undefined) {
			partitionMapper[id][t] = RAM[id].length;
			RAM[id].push({data: []});							// la partition n'existe pas on la crée
		}
		if (head) {
			// write file to disk
			var pre = fs.createWriteStream('/tmp/UGRID_RAM/' + id + '/' + grid.host.uuid + '/0.pre');
			for (i = 0; i < this.tmp.length; i++)
				pre.write(JSON.stringify(this.tmp[i]) + '\n');
		} else
			for (i = 0; i < this.tmp.length; i++)
				partitionMapperToFS[id][t].write(JSON.stringify(this.tmp[i]) + '\n');
	};

	this.pipeline = function(p, head) {
		for (var t = 1; t < transform.length; t++) {
			this.tmp = node[transform[t]].transform.pipeline(this.tmp, p, node[transform[t - 1]].id);
			if (this.tmp && (this.tmp.length === 0)) return;
			if (node[transform[t]].persistent && (node[transform[t]].dependency == 'narrow'))
				this.save(t, head);
		}
		if (param.inLastStage) {
			job.action.pipeline(this.tmp, p, head);
		}
	};
}

var UgridSource = {
	parallelize: function(grid, app, job, stage, param) {
		Source.call(this, grid, app, job, stage, param);

		this.run = function(callback) {
			var input = job.node[param.transform[0]].args[0] || [];
	 		var persistent = job.node[param.transform[0]].persistent;
			for (var p = 0; p < input.length; p++) {
				var partition = input[p];
				for (var i = 0; i < partition.length; i++) {
					this.tmp = [partition[i]];
					if (persistent) this.save(0);
					this.pipeline(p);
				}
			}
			stage.next_target_cnt--;
			callback();
		};
	},
	stream: function(grid, app, job, stage, param) {
		Source.call(this, grid, app, job, stage, param);

	 	var cbk;
	 	var self = this;
	 	var persistent = job.node[param.transform[0]].persistent;
	 	var num = param.transform[0];
	 	var args = job.node[num].args;
	 	var N = args[0];
	 	var n = 0;
	 	var streamIdx = args[1];

		app.completedStreams[streamIdx] = false;
		grid.removeAllListeners(streamIdx);
		grid.removeAllListeners(streamIdx + '.end');

		grid.on(streamIdx, function(data, done) {
			self.tmp = [data];
			if (persistent) self.save(0);
			self.pipeline(0);
			if (++n < N) {
				done();
			} else {
				n = 0;
				app.dones[streamIdx] = done;				
				cbk();
			}
		});

		grid.on(streamIdx + '.end', function(ignore, done) {
			grid.removeAllListeners(streamIdx);
			grid.removeAllListeners(streamIdx + '.end');
			app.completedStreams[streamIdx] = true;
			stage.next_target_cnt--;
			if (ignore) job.action.semaphore = 1;
			job.action.ignore = ignore;
			app.dones[streamIdx] = done;
			cbk(ignore);
		});

		this.run = function (callback) {cbk = callback;};
	},
	fromRAM: function(grid, app, job, stage, param) {
		Source.call(this, grid, app, job, stage, param);
		var self = this;

		this.run = function(callback) {
			stage.locked = true;

			function readFile(path, p, end, cbk) {
				var lines = new Lines();
				if (fs.existsSync(path)) {
					fs.createReadStream(path, {encoding: 'utf8'}).pipe(lines);
					lines.on('data', function(line) {
						self.tmp = [JSON.parse(line)];
						self.pipeline(0);
					});

					lines.on('end', function() {
						if (end) {
							stage.locked = false;
							stage.next_target_cnt--;
						}
						cbk();
					})
				} else {
					if (end) {
						stage.locked = false;
						stage.next_target_cnt--;
					}					
					cbk();
				}
			}
			// Read pre then partition
			var p = 0;
			var path = '/tmp/UGRID_RAM/' + job.node[param.transform[0]].id + '/' + grid.host.uuid + '/' + p + '.pre';
			readFile(path, p, false, function () {
				var p = 0;
				var path = '/tmp/UGRID_RAM/' + job.node[param.transform[0]].id + '/' + grid.host.uuid + '/' + p;
				readFile(path, p, true, callback);
			});
		};
	},
	fromSTAGERAM: function(grid, app, job, stage, param) {
		Source.call(this, grid, app, job, stage, param);

		this.run = function(callback) {
			var input = job.node[param.transform[0]].transform.SRAM || [];
	 		var persistent = job.node[param.transform[0]].persistent;
			for (var p = 0; p < input.length; p++) {
				var partition = input[p].data;
				for (var i = 0; i < partition.length; i++) {
					this.tmp = [partition[i]];
					if (persistent) this.save(0);
					this.pipeline(p);
				}
			}
			stage.next_target_cnt--;
			callback();
		};
	},
	randomSVMData: function(grid, app, job, stage, param) {
		Source.call(this, grid, app, job, stage, param);

		this.run = function(callback) {
			var num = param.transform[0];
			var D = job.node[num].args[0];
			var partition = job.node[num].args[1] || [];
			var persistent = job.node[num].persistent;
			for (var p = 0; p < partition.length; p++) {
				var rng = new ml.Random(partition[p].seed);
				for (var i = 0; i < partition[p].n; i++) {
					this.tmp = [ml.randomSVMLine(rng, D)];
					if (persistent) this.save(0);
					this.pipeline(p);
				}
			}
			stage.next_target_cnt--;
			callback();
		};
	},
	textFile: function(grid, app, job, stage, param) {
		Source.call(this, grid, app, job, stage, param);

	 	var num = param.transform[0];
		var cbk;
		var self = this;
		var persistent = job.node[num].persistent;
	 	var blocks = job.node[num].args[2];
		var hashedBlocks = {};
		var blockIdx = 0;

		for (var i = 0; i < blocks.length; i++) {
			blocks[i].p = i;
			hashedBlocks[blocks[i].bid] = blocks[i];
		}

		this.run = function(callback) {
			cbk = callback;
			if (blocks.length === 0) callback();
			else processBlock(blockIdx);
		};

		function processLine(p, line, first) {
			self.tmp = [line];
			if (persistent) self.save(0, first);
			self.pipeline(p, first);
		}

		function processBlock(bid) {
			var lines = new Lines();
			fs.createReadStream(blocks[bid].file, blocks[bid].opt).pipe(lines);
			stage.locked = true;

			lines.once("data", function (line) {
				blocks[bid].firstLine = line;
				lines.once("data", function (line) {
					blocks[bid].lastLine = line;
					lines.on("data", function (line) {
						self.tmp = [blocks[bid].lastLine];
						if (persistent) self.save(0);
						self.pipeline(blocks[bid].p);
						blocks[bid].lastLine = line;
					});
				});
			});

			function shuffleLine(shuffledLine) {
				var shuffleTo = blocks[bid].shuffleTo;
				if (shuffleTo != app.wid) {
					grid.send(app.worker[shuffleTo].uuid, {cmd: 'lastLine', args: {lastLine: shuffledLine, sid: stage.sid, lid: param.lid, bid: blocks[bid].bid + 1}, jobId: job.id});
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
					stage.locked = false;
					stage.next_target_cnt--;
					cbk();
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
				stage.locked = false;
				stage.next_target_cnt--;
				cbk();
			}
		};

		function processFirstLine(bid) {
			var targetBlock = hashedBlocks[bid];
			if (!targetBlock.hasReiceivedLastLine || !targetBlock.hasScannedFile) return false;
			if (targetBlock.forward && targetBlock.shuffleLastLine) {
				var shuffledLine = targetBlock.rxLastLine + targetBlock.firstLine;
				if (targetBlock.shuffleTo != app.wid) {
					grid.send(app.worker[targetBlock.shuffleTo].uuid, {cmd: 'lastLine', args: {lastLine: shuffledLine, sid: stage.sid, lid: param.lid, bid: bid + 1}});
				} else processLastLine({lastLine: shuffledLine, bid: bid + 1});
			} else {
				var str = (targetBlock.rxLastLine === undefined) ? targetBlock.firstLine : targetBlock.rxLastLine + targetBlock.firstLine;
				processLine(targetBlock.p, str, true);
			}
			return true;
		}
	}
}
