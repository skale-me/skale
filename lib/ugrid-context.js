'use strict';

var util = require('util');
var thunkify = require('thunkify');
var url = require('url');

var UgridClient = require('./ugrid-client.js');
var UgridArray = require('./ugrid-array.js');

module.exports = UgridContext;

util.inherits(UgridContext, UgridClient);

function UgridContext(arg) {
	if (!(this instanceof UgridContext))
		return new UgridContext(arg);

	var self = this, worker = this.worker = [];
	var grid = this.grid = new UgridClient({
		host: (arg && arg.host) || process.env.UGRID_HOST || 'localhost',
		port: (arg && arg.port) || process.env.UGRID_PORT || 12346,
		data: (arg && arg.data) || {type: 'master'}
	});

	this.init_cb = function (callback) {
		//grid.connect_cb(function () {
			grid.devices_cb({type: 'worker'}, function(err, res) {
				for (var i = 0; i < res.length; i++)
					self.worker[i] = new Worker(res[i]);
				callback();
			});
		//});
	};

	function Worker(w) {
		this.uuid = w.uuid;
		this.id = w.id;
		this.ip = w.ip;
	}

	Worker.prototype.rpc = function (cmd, args, callback) {
		grid.request_cb({uuid: this.uuid, id: this.id}, {cmd: cmd, args: args}, callback);
	};

	this.end = function () {
		grid.end();
	};

	this.randomSVMData = function (N, D, seed, nPartitions) {
		// by default number of partitions equals number of workers
		var i, p, partitions = [], data = [], acc, wid = 0;
		var P = nPartitions || worker.length;

		// Create partitions array
		for (p = 0; p < P; p++) partitions.push({n: 0});
		// Set partitions length
		p = 0;
		for (i = 0; i < N; i++) {
			partitions[p].n++;
			p = (p + 1) % P;
		}
		// Set partitions seed
		acc = seed;
		for (p = 0; p < P; p++) {
			partitions[p].seed = acc;
			acc += partitions[p].n * (D + 1);
		}
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = {};
			data[wid][p] = partitions[p];
			wid = (wid + 1) % worker.length;
		}

		var a = new UgridArray(grid, worker, [], 'narrow', 'randomSVMData', [data]);
		a.getArgs = function(wid) {
			return {args: [D, a.args[0][wid]]};
		}
		a.inputSource = function () {
			function randomSVMData(num, persistent, id, sid) {
				var tmp, p, i, rng;
				var D = node[num].args[0];
				var partition = node[num].args[1];
				if (persistent) {
					RAM[id] = {};
					for (p in partition) RAM[id][p] = [];
				}
				for (p in partition) {
					rng = new ml.Random(partition[p].seed);
					for (i = 0; i < partition[p].n; i++) {
						tmp = ml.randomSVMLine(rng, D);
						"PIPELINE_HERE"
					}
				}
				stage_cnt[sid]++;
			}
			return randomSVMData.toString() + ';' +
				'randomSVMData(' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ', ' + a.stageIdx + ');'
		}
		return a;
	};

	this.parallelize = function (localArray, nPartitions) {
		// by default number of partitions equals number of workers
		var i, p, partitions = [], data = [], wid = 0;
		var P = nPartitions || worker.length;

		// Create partitions array
		for (p = 0; p < P; p++) partitions.push([]);
		// Fill partitions with localArray data
		p = 0;
		for (i = 0; i < localArray.length; i++) {
			partitions[p].push(localArray[i]);
			p = (p + 1) % P;
		}
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = {};
			data[wid][p] = partitions[p];
			wid = (wid + 1) % worker.length;
		}

		var a = new UgridArray(grid, worker, [], 'narrow', 'parallelize', [data]);
		a.getArgs = function(wid) {
			return {args: [a.args[0][wid]]};
		}
		a.inputSource = function () {
			function parallelize(num, persistent, id, sid) {
				var partition = node[num].args[0];
				if (persistent) {
					RAM[id] = {};
					for (var p in partition) RAM[id][p] = [];
				}
				for (var p in partition) {
					for (var i = 0; i < partition[p].length; i++) {
						tmp = partition[p][i];
						"PIPELINE_HERE"
					}
				}
				stage_cnt[sid]++;
			}
			return parallelize.toString() + ';' +
				'parallelize(' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ', ' + a.stageIdx + ');';
		}
		return a;
	};

	this.textFile = function (path, nPartitions) {
		var u = url.parse(path);
		// hdfs file
		if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
			var a = new UgridArray(grid, worker, [], 'narrow', 'hdfsTextFile', [u.path]);
			a.getArgs = function(wid) {
				// here args has been modified during prebuildTask
				// hence it can't be deducted yet from path
				return {args: [a.args[0][wid]]};
			}
			a.inputSource = function () {
				function hdfsTextFile(blockIdx, num, persistent, id, stageIdx) {
					var file = node[num].args[0][blockIdx].file;
					var partitionIdx = [node[num].args[0][blockIdx].blockNum];
					var partitionLength = {};
					for (var p = 0; p < partitionIdx.length; p++)
						partitionLength[partitionIdx[p]] = 0;
					if (persistent) {
						RAM[id] = {};
						for (var p = 0; p < partitionIdx.length; p++)
							RAM[id][partitionIdx[p]] = [];
					}
					stage_locked[stageIdx] = true;
					var lines = new Lines();
					fs.createReadStream(file).pipe(lines);

					var skipFirstLine = (node[num].args[0][blockIdx].blockNum == 0) ? false : true;
					var shuffleLastLine = (node[num].args[0][blockIdx].blockNum == (node[num].args[0].length - 1)) ? false : true;
					var lastline, firstline, tmp;

					function processLine(line) {
						lastline = line;
						lines.on("data", function (line) {
							var i = partitionLength[partitionIdx[0]]++;
							tmp = lastline;
							"PIPELINE_HERE"
							lastline = line;
						});
					}

					// Skip first line PIPELINE and introduce one line delay
					if (skipFirstLine) {
						console.log('skipping first line of block ' + node[num].args[0][blockIdx].blockNum)
						lines.once("data", function (line) {
							// do something with the first line here
							firstline = line;
							lines.once("data", processLine);
						});
					} else
						lines.once("data", processLine);

					lines.on("end", function() {
						// Shuffle last line if needed and continue to next block
						if (shuffleLastLine)
							console.log('block: ' + node[num].args[0][blockIdx].blockNum + ', Need to shuffle ' + lastline);

						stage_locked[stageIdx] = false;
						if (++blockIdx < node[num].args[0].length) {
							hdfsTextFile(blockIdx, num, persistent, id, stageIdx);
						} else if (stage_length[stageIdx] == ++stage_cnt[stageIdx])
							callback(res);
					});
				}

				return 	hdfsTextFile.toString() + ';' +
					'hdfsTextFile(0, ' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ', ' + a.stageIdx + ');';
			}
			return a;
		}

		// local file
		// by default number of partitions equals number of workers
		var i, p, data = [], wid = 0;
		var P = nPartitions || worker.length;

		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = [];
			data[wid].push(p);
			wid = (wid + 1) % worker.length;
		}
		// Padd data with zero length arrays if needed
		if (P < worker.length)
			for (i = P; i < worker.length; i++)
				data[i] = [];

		var a = new UgridArray(grid, worker, [], 'narrow', 'textFile', [data]);
		a.getArgs = function(wid) {
			return {args: [path, P, a.args[0][wid]]};
		}
		a.inputSource = function () {
			function textFile(num, persistent, id, stageIdx) { // attributes of node a
				var file = node[num].args[0];
				var P = node[num].args[1];
				var partitionIdx = node[num].args[2];
				var partitionLength = {};
				for (var p = 0; p < partitionIdx.length; p++)
					partitionLength[partitionIdx[p]] = 0;
				if (persistent) {
					RAM[id] = {};
					for (var p = 0; p < partitionIdx.length; p++)
						RAM[id][partitionIdx[p]] = [];
				}
				var l = 0;
				stage_locked[stageIdx] = true;
				var lines = new Lines();
				fs.createReadStream(file).pipe(lines);
				lines.on("data", function(tmp) {
					var p = l++ % P;
					if (partitionIdx.indexOf(p) != -1) {
						var i = partitionLength[p] ++;
						"PIPELINE_HERE"
					}
				});
				lines.on("end", function() {
					stage_locked[stageIdx] = false;
					if (stage_length[stageIdx] == ++stage_cnt[stageIdx])
						callback(res);
				});
			}			
			return textFile.toString() + ';' +
				'textFile(' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ', ' + a.stageIdx + ');';
		}
		return a;
	}

	this.init = thunkify(this.init_cb);
}
