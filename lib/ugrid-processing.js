'use strict';

var fs = require('fs');
var RDD = require('./ugrid-transformation.js');
var trace = require('line-trace');
var ml = require('./ugrid-ml.js');
var Lines = require('./lines.js');

module.exports.UgridJob = function(grid, app, param) {
	var stageIdx = 0, stage = [], nStage = param.stageData.length;
	var job = {id: param.jobId, node: param.node};
	var action = job.action = new RDD[param.actionData.fun](grid, app, job, param.actionData);

	for (var i = 0; i < nStage; i++) {
		var stageParam = {sid: i, stageData: param.stageData[i], isLastStage: i == (nStage - 1)};
		stage[i] = new Stage(grid, app, job, stageParam, nextStage);
	}

	function nextStage(sid, ignore) {
		var i, n, s, jobFinished;
		if (sid != stageIdx) return;
		if (sid == (nStage - 1)) {
			if ((app.wid === 0) || (++action.semaphore == 2)) {
				if (!ignore) {
					action.sendResult();
					// reset action and nodes
					action.reset();
					for (n in job.node)
						if (n.reset) n.reset();
					// set next iteration starting stage
					for (i = 0; i < stage.length; i++)
						if (stage[i].state.next_target_cnt !== 0) break;
					if (i < stage.length) stageIdx = i;
					else console.log('Job iteration is finished');
				}
				// unlock distant streams
				for (i in app.dones) 
					app.dones[i]();
				app.dones = {};
				// Stream is finished not at a block frontier
				// job is finished if all stages are finished
				jobFinished = true;
				for (s in app.completedStreams) {
					if (!app.completedStreams[s]) {
						jobFinished = false;
						break;
					}
				}
				if (jobFinished) {
					grid.send(app.master_uuid, {cmd: 'endJob', data: param.jobId});
					console.log('Job is finished')
				}
				stage[sid].target_cnt = stage[sid].next_target_cnt;
				stage[sid].cnt = 0;
			}
		} else if (ignore === true) {
			// We got endJob at block frontier
			// unlock distant streams
			for (i in app.dones) app.dones[i]();
			app.dones = {};
			// job is finished if all stages are finished
			jobFinished = true;
			for (s in app.completedStreams) {
				if (!app.completedStreams[s]) {
					jobFinished = false;
					break;
				}
			}
			if (jobFinished)
				grid.send(app.master_uuid, {cmd: 'endJob', data: param.jobId});
		} else if (stage[stageIdx].state.nShuffle == app.worker.length)
			stage[++stageIdx].run();
	}

	this.run = function() {stage[0].run();};

	this.processShuffle = function(msg) {
		stage[msg.sid].processShuffle(msg, nextStage);
	};

	this.processAction = function() {
		if (action.semaphore == 1)
			nextStage(stage.length - 1);
		else ++action.semaphore;
	};

	this.processLastLine = function(msg) {
		stage[msg.args.sid].processLastLine(msg);
	};
};

function Stage(grid, app, job, param, nextStage) {
	var node = job.node;
	var lineages = param.stageData.lineages;
	var shuffleNum = param.stageData.shuffleNum;
	var source = [];	
	var state = this.state = {
		cnt: 0,								// Number of finished lineages
		target_cnt: lineages.length,		// Number of lineages
		locked: false,						// Because of an asynchonous lineage
		nShuffle: 0,						// Number of shuffle received
		sid: param.sid,						// Indice du stage
		next_target_cnt: lineages.length	// next iteration lineage target count
	};

	for (var l = 0; l < lineages.length; l++) {
		for (var j = 1; j < lineages[l].length; j++) {
			var n = node[lineages[l][j]];
			if (typeof(n.transform) == 'string')
				n.transform = new RDD[n.transform](grid, app, job, state, n);
		}
		var sourceParam = {lid: l, transform: lineages[l], inLastStage: param.isLastStage};
		source[l] = new UgridSource[node[lineages[l][0]].type](grid, app, job, state, sourceParam);
	}

	this.run = function () {
		for (var l = 0; l < source.length; l++) {
			try {
				source[l].run(function(streamIdx, done, ignore) {
					if (done) app.dones[streamIdx] = done;
					if ((++state.cnt < state.target_cnt) || state.locked) return;
					if (!param.isLastStage) {
						node[shuffleNum].transform.tx_shuffle();	// not last stage
					}
					nextStage(state.sid, ignore);
				});
			} catch (err) {
				console.error(err.stack);
				throw new Error("Lineage error, " + err);
			}
		}
	};

	this.processShuffle = function (msg) {
		try {node[shuffleNum].transform.rx_shuffle(msg.args);} 
		catch (err) {throw new Error("Lineage rx shuffle " + node[shuffleNum].type + ": " + err);}
		nextStage(state.sid);
	};

	this.processLastLine = function (msg) {
		source[msg.args.lid].processLastLine(msg.args);
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
			partitionMapper[id] = [];						// Nouveau vecteur associé au lineage
			partitionMapper[id][t] = 0;
			RAM[id] = [{data: []}];							// nouveau vecteur de partition dans la RAM
			partitionMapperToFS[id] = [];
			try {fs.mkdirSync(RAM_DIR);} 
			catch (e) {console.log(RAM_DIR + " exists")};
			try {fs.mkdirSync(RAM_DIR + id);} 
			catch (e) {console.log(RAM_DIR + id + " exists")};
			try {fs.mkdirSync(RAM_DIR + id + '/' + grid.host.uuid);} 
			catch (e) {console.log(RAM_DIR + id + '/' + grid.host.uuid + " exists")};
			partitionMapperToFS[id][t] = fs.createWriteStream('/tmp/UGRID_RAM/' + id + '/' + grid.host.uuid + '/0');
		} else if (partitionMapper[id][t] === undefined) {
			partitionMapper[id][t] = RAM[id].length;
			RAM[id].push({data: []});						// la partition n'existe pas on la crée
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
				cbk(streamIdx, done);
			}
		});

		grid.on(streamIdx + '.end', function(ignore, done) {
			// console.log('End of stream, ignore: ' + ignore);
			grid.removeAllListeners(streamIdx);
			grid.removeAllListeners(streamIdx + '.end');
			app.completedStreams[streamIdx] = true;
			stage.next_target_cnt--;
			if (ignore) job.action.semaphore = 1;
			cbk(streamIdx, done, ignore);
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
				} else cbk();
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
