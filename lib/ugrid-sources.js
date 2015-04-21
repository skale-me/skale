'use strict';

var fs = require('fs');
var Connection = require('ssh2');
var url = require('url');
var MongoClient = require('mongodb').MongoClient;
var stream = require('stream');
var util = require('util');

var ml = require('./ugrid-ml.js');
var Lines = require('./lines.js');

function Source(lid, grid, worker, state, node, RAM, transform, action) {
	var partitionMapper = {};
	this.tmp = [];

	this.save = function(t, head) {
		var id = node[transform[t]].id;
		if (partitionMapper[id] == undefined) {			// le dataset id n'existe pas, on le crée
			partitionMapper[id] = [];						// Nouveau vecteur associé au lineage
			partitionMapper[id][t] = 0;
			RAM[id] = [{data: []}];						// nouveau vecteur de partition dans la RAM
		} else if (partitionMapper[id][t] == undefined) {
			partitionMapper[id][t] = RAM[id].length;
			RAM[id].push({data: []});						// la partition n'existe pas on la crée
		}
		// on récupère l'indice de la partition dans laquelle stocker les datas
		var idx = partitionMapper[id][t];
		var t0 = RAM[id][idx].data;
		var L = t0.length;

		if (head)
			for (var i = this.tmp.length - 1; i >= 0; i--) t0.unshift(this.tmp[i]);
		else
			for (var i = 0; i < this.tmp.length; i++) t0[L + i] = this.tmp[i];
	}

	this.pipeline = function(p, head) {
		for (var t = 1; t < transform.length; t++) {
			this.tmp = node[transform[t]].transform.pipeline(this.tmp, p, node[transform[t - 1]].id);			
			if (this.tmp && (this.tmp.length == 0)) return;
			if (node[transform[t]].persistent && (node[transform[t]].dependency == 'narrow'))
				this.save(t, head);
		}
		action && action.pipeline(this.tmp, p, head);
	}
}

module.exports.stream = function(lid, grid, worker, state, node, RAM, transform, action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action);

 	var cbk;
 	var self = this;
 	var persistent = node[transform[0]].persistent;
 	var num = transform[0];
 	var args = node[num].args
 	// console.log(args)

	grid.on('unique_stream_id', function(msg) {
		console.log('Received msg')
		console.log(msg);
		self.tmp = msg.data;
		if (persistent) self.save(0);
		self.pipeline(0);
		// Si la condition de trig de l'action est atteint ici, on appelle this.endStage(callback) 
		cbk();
	})

	this.run = function(callback) {cbk = callback;}

}

module.exports.parallelize = function(lid, grid, worker, state, node, RAM, transform, action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action);

	this.run = function(callback) {
		var input = node[transform[0]].args[0] || [];
 		var persistent = node[transform[0]].persistent;
		for (var p = 0; p < input.length; p++) {			
			var partition = input[p];			
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				if (persistent) this.save(0);
				this.pipeline(p);
			}
		}
		callback();
	}
}

module.exports.fromRAM = function(lid, grid, worker, state, node, RAM, transform, action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action);

	this.run = function(callback) {
		var input = RAM[node[transform[0]].id] || [];		
		for (var p = 0; p < input.length; p++) {
			var partition = input[p].data;
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				this.pipeline(p);
			}
		}
		callback();
	}
}

module.exports.fromSTAGERAM = function(lid, grid, worker, state, node, RAM, transform, action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action);

	this.run = function(callback) {
		var input = node[transform[0]].transform.SRAM || [];
 		var persistent = node[transform[0]].persistent;
		for (var p = 0; p < input.length; p++) {
			var partition = input[p].data;
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				if (persistent) this.save(0);
				this.pipeline(p);
			}
		}
		callback();
	}
}

module.exports.randomSVMData = function(lid, grid, worker, state, node, RAM, transform, action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action);

	this.run = function(callback) {
		var num = transform[0];
		var D = node[num].args[0];
		var partition = node[num].args[1] || [];
		var persistent = node[num].persistent;
		for (var p = 0; p < partition.length; p++) {
			var rng = new ml.Random(partition[p].seed);
			for (var i = 0; i < partition[p].n; i++) {
				this.tmp = [ml.randomSVMLine(rng, D)];
				if (persistent) this.save(0);
				this.pipeline(p);
			}
		}
		callback();
	}
}

module.exports.mongo = function(lid, grid, worker, state, node, RAM, transform, action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action);

 	var num = transform[0];
 	var url = node[num].args[0];
 	var query = node[num].args[1];
 	var self = this;
 	var p = 0;
 	var persistent = node[num].persistent;	
	var skip = 0, entriesPerWorker = 0, cbk, ready = false;

	for (var wid = 0; wid < worker.length; wid++)
		if (worker[wid].uuid == grid.host.uuid) break;

	MongoClient.connect(url, function(err, db) {
		if (err) throw new Error(err);
		db.collection('ugrid').count(query, function(err, res) {
			entriesPerWorker = Math.ceil(res / worker.length);
			skip = entriesPerWorker * wid;
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
				callback();
			});
		});
	}
}

module.exports.textFile = function(lid, grid, worker, state, node, RAM, transform, action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action);

 	var num = transform[0];
	var ready = false, cbk;
	var self = this;
 	var persistent = node[num].persistent;	
	var u = url.parse(node[num].args[0]);
	var opt, file, blocks = [], hashedBlocks = {};
	var blockIdx = 0;

	for (var wid = 0; wid < worker.length; wid++)
		if (worker[wid].uuid == grid.host.uuid) break;

	function hdfs(file, callback) {
		// tenter la connexion en ssh
		// si la connexion n'est pas possible, on passe alors ar webhdfs
		// sans exploiter la localisation des données
		// Recuperer valeur de host au sein de 'hdfs://localhost:9000/test/data.txt', arg de l'API hdfs
		var host = process.env.HDFS_HOST;
		var username = process.env.HDFS_USER;
		var privateKey = process.env.HOME + '/.ssh/id_rsa';
		var bd = process.env.HADOOP_PREFIX;
		var data_dir = process.env.HDFS_DATA_DIR;

		var fsck_cmd = bd + '/bin/hadoop fsck ' + file + ' -files -blocks -locations';
		// var regexp_1_2 = /(\d+\. blk_-*\d+_\d+ len=\d+ repl=\d+ [[][0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+.*\])/i;
		var regexp = /(\d+\. .*blk_-*\d+_\d+ len=\d+ repl=\d+ [[][0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+.*\])/i;
		var blocks = [];
		var conn = new Connection();

		conn.on('ready', function() {
			conn.exec(fsck_cmd, function(err, stream) {
				if (err) throw new Error(err);
				var lines = new Lines();
				stream.stdout.pipe(lines);
				var nBlock = 0;
				lines.on('data', function(line) {
					// Filter fsck command output
					if (line.search(regexp) == -1) return;
					var v = line.split(' ');
					// Build host list for current block
					var host = [];
					for (var i = 4; i < v.length; i++)
						host.push(v[i].substr(0, v[i].lastIndexOf(':')).replace('[', ''));
					// Map block to less busy worker
					blocks.push({
						file: data_dir + '/' + v[1].substr(0, v[1].lastIndexOf('_')).replace(':', '/current/finalized/subdir0/subdir0/'),
						host: host,
						opt: {},
						skipFirstLine: blocks.length ? true : false,
						shuffleLastLine: true,
						bid: nBlock++
					});
				});
				lines.on('end', function() {
					conn.end();
					blocks[blocks.length - 1].shuffleLastLine = false;
					// Each block can be located on multiple slaves
					var mapping = {}, min_id, host;
					for (var i = 0; i < worker.length; i++) {
						worker[i].ip = worker[i].ip.replace('::ffff:', '');	// WORKAROUND ICI, ipv6 pour les worker
						if (mapping[worker[i].ip] == undefined)
							mapping[worker[i].ip] = {};
						mapping[worker[i].ip][i] = [];
					}

					// map each block to the least busy worker on same host
					for (var i = 0; i < blocks.length; i++) {
						min_id = undefined;
						// boucle sur les hosts du block
						for (var j = 0; j < blocks[i].host.length; j++)
							for (var w in mapping[blocks[i].host[j]]) {
								if (min_id == undefined) {
									min_id = w;
									host = blocks[i].host[j];
									continue;
								}
								if (mapping[blocks[i].host[j]][w].length < mapping[host][min_id].length) {
									min_id = w;
									host = blocks[i].host[j];
								}
							}
						if (i > 0) blocks[i - 1].shuffleTo = parseInt(min_id);
						mapping[host][min_id].push(blocks[i]);
					}
					callback(mapping[worker[wid].ip][wid]);
				});
			});
		}).connect({
			host: host,
			username: username,
			privateKey: fs.readFileSync(privateKey)
		});
	}

	if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
		// HDFS		
	 	hdfs(u.path, function(data) {
	 		blocks = data;
	 		for (var i = 0; i < blocks.length; i++) {
	 			blocks[i].p = i;
	 			hashedBlocks[blocks[i].bid] = blocks[i];
	 		}
	 		if (ready) run(cbk);
	 		ready = true;
	 	});
	} else {
		// NFS
		ready = true;
		file = node[num].args[0];
		var size = fs.statSync(file).size;
		var base = Math.floor(size / worker.length);
		if (wid == worker.length - 1)
			var nBytes = size - base * (worker.length - 1);
		else
			var nBytes = base;
		opt = {start: wid * base, end: wid * base + nBytes - 1};
		blocks[0] = {file: file, opt: opt};
 		blocks[0].skipFirstLine = (wid == 0) ? false : true;
 		blocks[0].shuffleLastLine = (wid == worker.length - 1) ? false : true;
		blocks[0].shuffleTo = wid + 1;
		blocks[0].bid = wid;
		blocks[0].p = 0;
		hashedBlocks[wid] = blocks[0];
	}	

	var run = this.run = function(callback) {
		cbk = callback;
		if (!ready) {
			ready = true;
			return;
		}
		if (blocks.length == 0) {
			callback();
			return;
		}
		processBlock(blockIdx);
	}

	function processLine(p, line, first) {
		self.tmp = [line];
		if (persistent) self.save(0, first);
		self.pipeline(p, first);
	}

	// il faut différencier bid local du bid global
	function processBlock(bid) {
		var lines = new Lines();
		fs.createReadStream(blocks[bid].file, blocks[bid].opt).pipe(lines);
		var skipFirstLine = blocks[bid].skipFirstLine;
		var shuffleLastLine = blocks[bid].shuffleLastLine;
		var shuffleTo = blocks[bid].shuffleTo;
		blocks[bid].nLines = 0;
		state.locked = true;

		lines.once("data", function (line) {
			blocks[bid].firstLine = line;
			blocks[bid].nLines++;
			lines.once("data", function (line) {
				blocks[bid].lastLine = line;
				blocks[bid].nLines++;
				lines.on("data", function (line) {
					blocks[bid].nLines++;
					self.tmp = [blocks[bid].lastLine];
					if (persistent) self.save(0);
					self.pipeline(blocks[bid].p);
					blocks[bid].lastLine = line;
				});
			});
		});

		function shuffleLine(shuffledLine) {
			if (shuffleTo != wid) {
				grid.request(worker[shuffleTo], {cmd: 'lastLine', args: {lastLine: shuffledLine, lid: lid, bid: blocks[bid].bid + 1}}, 
					function(err) {if (err) throw new Error(err);});
			} else {
				processLastLine({lastLine: shuffledLine, bid: blocks[bid].bid + 1});
			}
		}

		lines.on("endNewline", function(lastLineComplete) {
			var firstLineProcessed, same;
			var isFirstBlock = (skipFirstLine == false);
			var isLastBlock = (shuffleLastLine == false);
			var hasLastLine = blocks[bid].lastLine != undefined;

			blocks[bid].hasScannedFile = true;
			// FIRST LINE
			if (isFirstBlock) {
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
				state.locked = false;
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
			state.locked = false;
			cbk();
		}
	}

	function processFirstLine(bid) {
		var targetBlock = hashedBlocks[bid];
		if (!targetBlock.hasReiceivedLastLine || !targetBlock.hasScannedFile) return false;
		if (targetBlock.forward && targetBlock.shuffleLastLine) {
			var shuffledLine = targetBlock.rxLastLine + targetBlock.firstLine;
			if (targetBlock.shuffleTo != wid) {
				grid.request(worker[targetBlock.shuffleTo], {cmd: 'lastLine', args: {lastLine: shuffledLine, lid: lid, bid: bid + 1}}, 
					function(err) {if (err) throw new Error(err);});
			} else processLastLine({lastLine: shuffledLine, bid: bid + 1});
		} else {
			var str = (targetBlock.rxLastLine == undefined) ? targetBlock.firstLine : targetBlock.rxLastLine + targetBlock.firstLine;
			processLine(targetBlock.p, str, true);
		}
		return true;
	}
}
