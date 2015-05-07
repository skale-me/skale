'use strict';

var fs = require('fs');
var trace = require('line-trace');
var url = require('url');
var MongoClient = require('mongodb').MongoClient;

var ml = require('./ugrid-ml.js');
var Lines = require('./lines.js');

function Source(grid, app, job, stage, param) {
	var node = job.node;
	var RAM = app.RAM;
	var partitionMapper = {};
	this.tmp = [];
	var transform = param.transform;

	this.save = function(t, head) {
		var i, id = node[transform[t]].id;
		if (partitionMapper[id] === undefined) {				// le dataset id n'existe pas, on le crée
			partitionMapper[id] = [];						// Nouveau vecteur associé au lineage
			partitionMapper[id][t] = 0;
			RAM[id] = [{data: []}];							// nouveau vecteur de partition dans la RAM
		} else if (partitionMapper[id][t] === undefined) {
			partitionMapper[id][t] = RAM[id].length;
			RAM[id].push({data: []});						// la partition n'existe pas on la crée
		}
		// on récupère l'indice de la partition dans laquelle stocker les datas
		var idx = partitionMapper[id][t];
		var t0 = RAM[id][idx].data;
		var L = t0.length;

		if (head)
			for (i = this.tmp.length - 1; i >= 0; i--) t0.unshift(this.tmp[i]);
		else
			for (i = 0; i < this.tmp.length; i++) t0[L + i] = this.tmp[i];
	};

	this.pipeline = function(p, head) {
		for (var t = 1; t < transform.length; t++) {
			this.tmp = node[transform[t]].transform.pipeline(this.tmp, p, node[transform[t - 1]].id);
			if (this.tmp && (this.tmp.length === 0)) return;
			if (node[transform[t]].persistent && (node[transform[t]].dependency == 'narrow'))
				this.save(t, head);
		}
		if (param.inLastStage) job.action.pipeline(this.tmp, p, head);
	};
}

module.exports.stream = function(grid, app, job, stage, param) {
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

	grid.on(streamIdx, function(msg, done) {
		self.tmp = [msg];
		if (persistent) self.save(0);
		self.pipeline(0);
		if (++n < N) done();
		else {
			n = 0;
			cbk(streamIdx, done);
		}
	});

	grid.on(streamIdx + '.end', function(ignore, done) {
		trace('on ' + streamIdx + '.end, ignore: ' + ignore);
		grid.removeAllListeners(streamIdx);
		grid.removeAllListeners(streamIdx + '.end');
		app.completedStreams[streamIdx] = true;
		stage.next_target_cnt--;
		cbk(streamIdx, done, ignore);
	});

	this.run = function (callback) {cbk = callback;};
};

module.exports.parallelize = function(grid, app, job, stage, param) {
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
};

module.exports.fromRAM = function(grid, app, job, stage, param) {
	Source.call(this, grid, app, job, stage, param);

	this.run = function(callback) {
		var input = app.RAM[job.node[param.transform[0]].id] || [];
		for (var p = 0; p < input.length; p++) {
			var partition = input[p].data;
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [JSON.parse(JSON.stringify(partition[i]))];
				this.pipeline(p);
			}
		}
		stage.next_target_cnt--;
		callback();
	};
};

module.exports.fromSTAGERAM = function(grid, app, job, stage, param) {
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
};

module.exports.randomSVMData = function(grid, app, job, stage, param) {
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
};

module.exports.mongo = function(grid, app, job, stage, param) {
	Source.call(this, grid, app, job, stage, param);

 	var num = param.transform[0];
 	var url = job.node[num].args[0];
 	var query = job.node[num].args[1];
 	var self = this;
 	var p = 0;
	var persistent = job.node[num].persistent;
	var skip = 0, entriesPerWorker = 0, cbk, ready = false;

	MongoClient.connect(url, function(err, db) {
		if (err) throw new Error(err);
		db.collection('ugrid').count(query, function(err, res) {
			entriesPerWorker = Math.ceil(res / app.worker.length);
			skip = entriesPerWorker * app.wid;
			if (ready) run(cbk);
			ready = true;
			db.close();
		});
	});

	var run = this.run = function(callback) {
		cbk = callback;
		if (!ready) {
			ready = true;
			return;
		}
		MongoClient.connect(url, function(err, db) {
			if (err) throw new Error(err);
			db.collection('ugrid').find(query).skip(skip).limit(entriesPerWorker).toArray(function(err, input) {
				for (var i = 0; i < input.length; i++) {
					self.tmp = [input[i]];
					if (persistent) self.save(0);
					self.pipeline(p);
				}
				db.close();
				stage.next_target_cnt--;
				callback();
			});
		});
	};
};

module.exports.textFile = function(grid, app, job, stage, param) {
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
};
