'use strict';

var fs = require('fs');
var Connection = require('ssh2');
var url = require('url');

var ml = require('./ugrid-ml.js');
var Lines = require('./lines.js');

module.exports.parallelize = function(lid, grid, worker, state, node, RAM, transform, action, fun_action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action, fun_action);

	this.run = function(callback) {
		var input = node[transform[0].num].args[0] || [];
		var persistent = transform[0].persistent;
		for (var p = 0; p < input.length; p++) {
			var partition = input[p];
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				if (persistent) this.save(0);
				this.pipeline(p);
			}
		}
		this.endStage(callback);
	}
}

module.exports.fromRAM = function(lid, grid, worker, state, node, RAM, transform, action, fun_action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action, fun_action);

	this.run = function(callback) {
		var input = RAM[transform[0].src_id] || [];
		for (var p = 0; p < input.length; p++) {
			var partition = input[p].data;
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				this.pipeline(p);
			}
		}
		this.endStage(callback);
	}
}

module.exports.fromSTAGERAM = function(lid, grid, worker, state, node, RAM, transform, action, fun_action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action, fun_action);

	this.run = function(callback) {
		var input = node[transform[0].num].transform.SRAM || [];
		var persistent = transform[0].persistent;
		for (var p = 0; p < input.length; p++) {
			var partition = input[p].data;
			for (var i = 0; i < partition.length; i++) {
				this.tmp = [partition[i]];
				if (persistent) this.save(0);
				this.pipeline(p);
			}
		}
		this.endStage(callback);
	}
}

module.exports.randomSVMData = function(lid, grid, worker, state, node, RAM, transform, action, fun_action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action, fun_action);

	this.run = function(callback) {
		var num = transform[0].num;
		var D = node[num].args[0];
		var partition = node[num].args[1] || [];
		var persistent = transform[0].persistent;
		for (var p = 0; p < partition.length; p++) {
			var rng = new ml.Random(partition[p].seed);
			for (var i = 0; i < partition[p].n; i++) {
				this.tmp = [ml.randomSVMLine(rng, D)];
				if (persistent) this.save(0);
				this.pipeline(p);
			}
		}
		this.endStage(callback);
	}
}

module.exports.textFile = function(lid, grid, worker, state, node, RAM, transform, action, fun_action) {
 	Source.call(this, lid, grid, worker, state, node, RAM, transform, action, fun_action);

 	var num = transform[0].num;
	var dest_id = transform[0].dest_id;
	var P = node[num].args[1];
	var lines = new Lines();
	var ready = false, cbk;
	var self = this;
	var rxLastLine, lastLine, firstLine;
	var persistent = transform[0].persistent;
	var p = 0;
	var u = url.parse(node[num].args[0]);
	var opt, file;

	for (var wid = 0; wid < worker.length; wid++)
		if (worker[wid].uuid == grid.host.uuid) break;

	if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
		// HDFS
	 	hdfs(u.path, function(blocks) {
	 		file = blocks[0].file;
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
	}

	var run = this.run = function(callback) {
		cbk = callback;
		if (!ready) {
			ready = true;
			return;
		}
		fs.createReadStream(file, opt).pipe(lines);
		// si premiere partition du premier worker -> la premiere ligne est processed normalement
		// si wid + 1 existe on envoie la dernière ligne au worker gérant la partition suivante
		state.locked = true;
		var skipFirstLine = ((p != 0) || (wid != 0)) ? true : false;
		var shuffleLastLine = worker[wid + 1] ? true : false;
		function processLine(line) {
			lastLine = line;
			lines.on("data", function (line) {
				self.tmp = [lastLine];
				if (persistent) self.save(0);
				self.pipeline(p);
				lastLine = line;
			});
		}
		lines.once("data", function (line) {
			if (skipFirstLine) {
				firstLine = line;
				lines.once("data", processLine);
			} else processLine(line);
		});
		lines.on("end", function() {
			// If first line is complete, process and finish
			if (rxLastLine) {
				self.tmp = [rxLastLine + firstLine];
				if (persistent) self.save(0);
				self.pipeline(p);
				state.locked = false;
				self.endStage(callback);
			}
			// Process or shuffle last line
			if (shuffleLastLine) {
				grid.request_cb(worker[wid + 1], {cmd: 'lastLine', args: {lastLine: lastLine, lid: lid}}, function(err) {if (err) throw err;});
			} else {
				self.tmp = [lastLine];
				if (persistent) self.save(0);
				self.pipeline(p);					
			}
			if (!skipFirstLine) {
				state.locked = false;
				self.endStage(callback);
			}
		});
	}

	this.processLastLine = function(lastLine) {
		console.log('received "' + lastLine + '"');
		console.log('firstLine "' + firstLine + '"');
		console.log('processing : "' + lastLine + firstLine + '"')
		if (firstLine == undefined) {
			rxLastLine = lastLine;
		} else {
			state.locked = false;
			this.tmp = [lastLine + firstLine];
			if (persistent) this.save(0, true);
			this.pipeline(p, true);
			this.endStage();
		}
	}

	function hdfs(file, callback) {
		// tenter la connexion en ssh
		// si la connexion n'est pas possible, on passe alors ar webhdfs
		// sans exploiter la localisation des données
		// Recuperer valeur de host au sein de 'hdfs://localhost:9000/test/data.txt', arg de l'API hdfs
		var host = process.env.HDFS_HOST || 'localhost';
		var username = process.env.HDFS_USER || 'cedricartigue';
		var privateKey = process.env.HOME + '/.ssh/id_rsa';
		var bd = process.env.HADOOP_PREFIX || '/usr/local/Cellar/hadoop/2.6.0';

		var fsck_cmd = bd + '/bin/hadoop fsck ' + file + ' -files -blocks -locations';
		// var regexp_1_2 = /(\d+\. blk_-*\d+_\d+ len=\d+ repl=\d+ [[][0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+.*\])/i;
		var regexp = /(\d+\. .*blk_-*\d+_\d+ len=\d+ repl=\d+ [[][0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+.*\])/i;		
		var data_dir = process.env.HDFS_DATA_DIR || '/usr/local/Cellar/hadoop/hdfs/tmp/dfs/data/current';
		var blocks = [];
		var conn = new Connection();
		
		conn.on('ready', function() {
			conn.exec(fsck_cmd, function(err, stream) {
				if (err) throw err;
				var lines = new Lines();
				stream.stdout.pipe(lines);
				lines.on('data', function(line) {
					if (line.search(regexp) == -1) return;
					// console.log(line)
					var v = line.split(' ');
					var host = [];
					for (var i = 4; i < v.length; i++)
						host.push(v[i].substr(0, v[i].lastIndexOf(':')).replace('[', ''));
					blocks.push({
						blockNum: parseFloat(v[0]),
						file: data_dir + '/' + v[1].substr(0, v[1].lastIndexOf('_')).replace(':', '/current/finalized/subdir0/subdir0/'),
						host: host
					});
				});
				lines.on('end', function() {
					conn.end();
					callback(blocks);
				});
			});
		}).connect({
			host: host,
			username: username,
			privateKey: fs.readFileSync(privateKey)
		});
	}

}

function Source(lid, grid, worker, state, node, RAM, transform, action, fun_action) {
	var partitionMapper = {};
	this.tmp = [];

	for (var wid = 0; wid < worker.length; wid++)
		if (worker[wid].uuid == grid.host.uuid) break;

	this.save = function(t, head) {
		var dest_id = transform[t].dest_id;
		if (partitionMapper[dest_id] == undefined) {			// le dataset dest_id n'existe pas, on le crée
			partitionMapper[dest_id] = [];						// Nouveau vecteur associé au lineage
			partitionMapper[dest_id][t] = 0;
			RAM[dest_id] = [{data: []}];						// nouveau vecteur de partition dans la RAM
		} else if (partitionMapper[dest_id][t] == undefined) {
			partitionMapper[dest_id][t] = RAM[dest_id].length;
			RAM[dest_id].push({data: []});						// la partition n'existe pas on la crée
		}
		// on récupère l'indice de la partition dans laquelle stocker les datas
		var idx = partitionMapper[dest_id][t];
		var t0 = RAM[dest_id][idx].data;
		var L = t0.length;

		if (head)
			for (var i = this.tmp.length - 1; i >= 0; i--) t0.unshift(this.tmp[i]);
		else
			for (var i = 0; i < this.tmp.length; i++) t0[L + i] = this.tmp[i];
	}

	this.pipeline = function(p, head) {
		for (var t = 1; t < transform.length; t++) {
			this.tmp = node[transform[t].num].transform.pipeline(this.tmp, p, transform[t].src_id);					
			if (this.tmp && (this.tmp.length == 0)) return;
			if (transform[t].persistent && (transform[t].dependency == 'narrow'))
				this.save(t, head);
		}
		action && fun_action.pipeline(this.tmp, p, head);
	}

	this.endStage = function(callback) {
		if (state.locked || (++state.cnt < state.target_cnt)) return;
		if (fun_action) {
			action.finished = true;
			if ((wid == 0) || action.unlocked) {
				action.callback(fun_action.result);
				if (worker[wid + 1])
					grid.request_cb(worker[wid + 1], {cmd: 'action', args: 'TEST'}, function(err) {if (err) throw err;});
			}
		} else {
			try {
				node[transform[transform.length - 1].num].transform.tx_shuffle(state);
				callback();
			} catch (err) {
				throw "Lineage tx shuffle " + transform[transform.length - 1].type + ": " + err;
			}
		}
	}	
}