'use strict';

var co = require('co');
var thenify = require('thenify');
var Connection = require('ssh2');
var url = require('url');
var fs = require('fs');

var ugridify  = require('./ugridify.js');
var trace = require('line-trace');
var Lines = require('./lines.js');

function UgridArray(grid, worker, child, dependency, transform, args, src) {
	this.id = Math.round(Math.random() * 1e9);			// Unique array id
	this.inMemory = false;
	this.child = child;
	this.persistent = false;
	this.visits = 0;
	this.transform = transform;
	this.dependency = dependency;
	this.args = args || [];

	for (var i = 0; i < child.length; i++)
		child[i].anc = this;	// set child ancestor

	function rpc(grid, cmd, workerNum, args, jobId, callback) {
		grid.request(worker[workerNum], {cmd: cmd, args: args, master_uuid: grid.uuid, jobId: jobId}, callback);
	}

	function sendJob(job, jobId, streamUgridArrays, callback) {
		grid.removeAllListeners(jobId);
		grid.on(jobId, function(result, done) {
			callback(null, result, done);
		});
		var n = 0;
		for (var i = 0; i < worker.length; i++)
			rpc(grid, 'setJob', i, job[i], jobId, function () {
				if (++n < worker.length) return;
				for (var j = 0; j < worker.length; j++)
					grid.send(worker[j].uuid, {cmd: 'runJob', data: {jobId: jobId}});
				for (var s in streamUgridArrays) {
					streamUgridArrays[s].startStream();
				}
			});
	}

	this.getArgs = function() {
		return {
			args: JSON.parse(JSON.stringify(this.args)),
			src: src,
			child: child.map(function(c) {return c.id;}),
			transform: this.transform
		};
	};

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'map', mapperArgs, mapper.toString());
	};

	this.keys = function () {
		var mapper = function (e) {return e[0];};
		return new UgridArray(grid, worker, [this], 'narrow', 'map', null, mapper.toString());
	};

	this.values = function () {
		var mapper = function (e) {return e[1];};
		return new UgridArray(grid, worker, [this], 'narrow', 'map', null, mapper.toString());
	};

	this.filter = function (filter, filterArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'filter', filterArgs, filter.toString());
	};

	this.flatMap = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'flatMap', mapperArgs, mapper.toString());
	};

	this.flatMapValues = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'flatMapValues', mapperArgs, mapper.toString());
	};

	this.sample = function (withReplacement, frac, seed) {
		return new UgridArray(grid, worker, [this], 'wide', 'sample', [withReplacement, frac, seed || 1]);
	};

	this.groupByKey = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'groupByKey', []);
	};

	this.reduceByKey = function (reducer, initVal) {
		return new UgridArray(grid, worker, [this], 'wide', 'reduceByKey', [initVal], reducer.toString());
	};

	this.union = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, worker, [this, other], 'narrow', 'union', [other.id]);
	};

	this.join = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, worker, [this, other], 'wide', 'join', [other.id, 'inner']);
	};

	this.leftOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, worker, [this, other], 'wide', 'join', [other.id, 'left']);
	};

	this.rightOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, worker, [this, other], 'wide', 'join', [other.id, 'right']);
	};

	this.coGroup = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, worker, [this, other], 'wide', 'coGroup', [other.id]);
	};

	this.crossProduct = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, worker, [this, other], 'wide', 'crossProduct', [other.id]);
	};

	this.mapValues = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'mapValues', mapperArgs, mapper.toString());
	};

	this.distinct = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'distinct', []);
	};

	this.sortByKey = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'sortByKey', []);
	};

	this.partitionByKey = function () {
		return new UgridArray(grid, worker, [this], 'wide', 'partitionByKey', []);
	};

	this.intersection = function (other) {
		return new UgridArray(grid, worker, [this, other], 'wide', 'intersection', []);
	};

	this.subtract = function (other) {
		return new UgridArray(grid, worker, [this, other], 'wide', 'subtract', []);
	};

	this.count = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		var jobId = grid.jobId++;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'count', args: [], init: 0, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, result = 0, dones = [];
			sendJob(job, jobId, streams, function (err, msg, done) {
				dones.push(done);
				result += msg;
				if (++nAnswer == worker.length) {
					if (opt.stream) {
						stream.write(result, function () {
							for (var i in dones) dones[i]();
							dones = [];
						});
					} else {
						callback(null, result);
						for (var i in dones) dones[i]();
						dones = [];
					}
					nAnswer = 0;
					result = 0;
				}
			});
		});
		return stream;
	});

	this.reduce = ugridify(function (reducer, aInit, opt, callback) {
		opt = opt || {};
		if (arguments.length < 4) callback = opt;
		var jobId = grid.jobId++;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'reduce', args: [aInit], src: reducer.toString(), init: aInit, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, dones = [];
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				aInit = reducer(aInit, res);
				if (++nAnswer == worker.length) {
					if (opt.stream) {
						stream.write(aInit, function () {
							for (var i in dones) dones[i]();
							dones = [];
						});
					} else {
						callback(null, aInit);
						for (var i in dones) dones[i]();
						dones = [];
					}
					nAnswer = 0;
				}
			});
		});
		return stream;
	});

	this.take = ugridify(function (num, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var jobId = grid.jobId++;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'take', args: [num], init: 0, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, result = [], dones = [];
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				for (var i = 0; i < res.length; i++)
					if (result.length < num) result.push(res[i]);
					else break;
				if (++nAnswer == worker.length) {
					if (opt.stream) {
						stream.write(result, function () {
							for (i in dones) dones[i]();
							dones = [];
						});
					} else {
						callback(null, result);
						for (i in dones) dones[i]();
						dones = [];
					}
					nAnswer = 0;
					result = [];
				}
			});
		});
		return stream;
	});

	this.top = ugridify(function (num, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var jobId = grid.jobId++;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		// By default sort order is on number
		var numsort = function (a, b) {return b - a;};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'top', args: [num, numsort], init: 0, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, result = [], dones = [];
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				result = result.concat(res);
				result = result.sort(numsort);
				result = result.slice(0, num);
				if (++nAnswer == worker.length) {
					if (opt.stream) {
						stream.write(result, function () {
							for (var i in dones) dones[i]();
							dones = [];
						});
					} else {
						callback(null, result);
						for (var i in dones) dones[i]();
						dones = [];
					}
					nAnswer = 0;
					result = [];
				}
			});
		});
		return stream;
	});

	this.takeOrdered = ugridify(function (num, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var jobId = grid.jobId++;
		// By default sort order is on number
		var numsort = function (a, b) {return a - b;};
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'takeOrdered', args: [num, numsort], init: 0, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, result = [], dones = [];
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				result = result.concat(res);
				result = result.sort(numsort);
				result = result.slice(0, num);
				if (++nAnswer == worker.length) {
					if (opt.stream) {
						stream.write(result, function () {
							for (var i in dones) dones[i]();
							dones = [];
						});
					} else {
						callback(null, result);
						for (var i in dones) dones[i]();
						dones = [];
					}
					nAnswer = 0;
					result = [];
				}
			});
		});
		return stream;
	});

	this.takeSample = ugridify(function (withReplacement, num, seed, opt, callback) {
		opt = opt || {};
		if (arguments.length < 5) callback = opt;
		var jobId = grid.jobId++;
		var self = this;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'count', args: [], init: 0, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, result = 0, dones = [];
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				result += res;
				if (++nAnswer == worker.length) {
					var a = new UgridArray(grid, worker, [self], 'wide', 'sample', [withReplacement, num / result, seed]);
					a.collect({stream: opt.stream, _stream: stream}, callback);
					nAnswer = 0;
					result = 0;
					for (var i in dones) dones[i]();
					dones = [];
				}
			});
		});
		return stream;
	});

	this.collect = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		var jobId = grid.jobId++;
		var stream = opt._stream || new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'collect', args: [], init: {}, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, vect = [], dones = [];

			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				for (var p in res) vect = vect.concat(res[p]);
				if (++nAnswer == worker.length) {
					if (opt.stream) {
						stream.write(vect, function () {
							for (var i in dones) dones[i]();
							dones = [];
						});
					} else { 
						callback(null, vect);
						for (var i in dones) dones[i]();
						dones = [];
					}
					vect = [];
					nAnswer = 0;
				}
			});
		});
		return stream;
	});

	this.lookup = ugridify(function (key, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var jobId = grid.jobId++;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'lookup', args: [key], init: {}, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, vect = [], dones = [];
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				for (var p in res) vect = vect.concat(res[p]);
				if (++nAnswer == worker.length) {
					if (opt.stream) {
						stream.write(vect, function () {
							for (var i in dones) dones[i]();
							dones = [];
						});
					} else {
						callback(null, vect);
						for (var i in dones) dones[i]();
						dones = [];
					}
					vect = [];
					nAnswer = 0;
					for (var i in dones) dones[i]();
					dones = [];
				}
			});
		});
		return stream;
	});

	this.countByValue = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var jobId = grid.jobId++;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'countByValue', args: [], init: {}, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, tmp = {}, dones = [], i;
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				for (i in res) {
					if (tmp[i] === undefined) tmp[i] = res[i];
					else tmp[i][1] += res[i][1];
				}
				if (++nAnswer == worker.length) {
					var vect = [];
					for (i in tmp) vect.push(tmp[i]);
					if (opt.stream) {
						stream.write(vect, function () {
							for (i in dones) dones[i]();
							dones = [];
						});
					} else {
						callback(null, vect);
						for (i in dones) dones[i]();
						dones = [];
					}
					nAnswer = 0;
					tmp = {};
				}
			});
		});
		return stream;
	});

	this.forEach = ugridify(function (eacher, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var jobId = grid.jobId++;
		var stream = new grid.createReadStream(jobId);
		grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
		if (!opt.stream) stream.once('end', callback);
		buildJob(worker, this, {fun: 'forEach', args: [], src: eacher.toString(), init: {}, jobId: jobId}, function (job, streams) {
			var nAnswer = 0, dones = [];
			sendJob(job, jobId, streams, function (err, res, done) {
				dones.push(done);
				if (++nAnswer == worker.length) {
					if (opt.stream) stream.write(null);
					else callback(null, null);
					nAnswer = 0;
					for (var i in dones) dones[i]();
					dones = [];
				}
			});
		});
		return stream;
	});
}

module.exports = UgridArray;

// ------------------------------------------------------------------------ //
// Job builder
// ------------------------------------------------------------------------ //
var treewalk = thenify(function (root, c_in, c_out, callback) {
	co(function *() {
		var n = root;
		if (c_in) yield c_in(n);
		while (1)
			if (n.child.length && (++n.visits <= n.child.length)) {
				n = n.child[n.visits - 1];
				if (c_in) yield c_in(n);
			} else {
				n.visits = 0;
				if (c_out) yield c_out(n);
				if (n == root) break;
				n = n.anc;
			}
		callback();
	}).catch(console.log);
});

function buildJob(worker, array, action, callback) {
	var nStage = 0, lastStage = 0, num = 0, streamUgridArrays = [];
	var job = new Array(worker.length);
	for (var i = 0; i < job.length; i++) job[i] = {node: {}};

	co(function *() {
		// Treewalk 1: Set stage index, node index and stage array
		yield treewalk(array, thenify(function(a, callback) {
			if (a.dependency == 'wide') nStage++;
			if (lastStage < nStage) lastStage = nStage;
			callback();
		}), thenify(function(a, callback) {
			a.sid = nStage;
			a.num = num++;
			if (a.dependency == 'wide') nStage--;
			if (a.transform == 'stream') streamUgridArrays.push(a);
			callback();
		}));
		var stageData = {};
		for (i = 0; i < lastStage + 1; i++)
			stageData[i] = {lineages: []};

		// Treewalk 2: Reverse stage index, identify lineages and set persistency
		yield treewalk(array, null, thenify(function(a, callback) {
			var c, i, last, type, nBytes;
			// Reverse stage index
			a.sid = lastStage - a.sid;
			if (a.dependency == 'wide') {
				stageData[a.sid].shuffleType = a.transform;
				stageData[a.sid].shuffleNum = a.num;
			}

			if (a.inMemory) {
				// if wide dependency discard stage up to this one and add a fromRAM to next stage
				if (a.dependency == 'wide') {
					for (i = 0; i <= a.sid; i++) delete stageData[i];
				} else {
					// if narrow dependency delete current stage lineages that lead to this transform
					// and add a fromRAM to this stage
					var lineagesToBeRemoved = [];
					for (i = 0; i < stageData[a.sid].lineages.length; i++) {
						last = stageData[a.sid].lineages[i].length - 1;
						for (c = 0; c < a.child.length; c++)
							if (a.child[c].num == stageData[a.sid].lineages[i][last]) {
								lineagesToBeRemoved.push(i);
								break;
							}
					}
					for (i = 0; i < lineagesToBeRemoved.length; i++)
						stageData[a.sid].lineages.splice(lineagesToBeRemoved[i], 1);
				}
				var sid = (a.dependency == 'wide') ? a.sid + 1 : a.sid;
				type = 'fromRAM';
				stageData[sid].lineages.push([a.num]);
			} else if (a.child.length === 0) {
				// If transform is a leaf, add a new lineage to current stage
				stageData[a.sid].lineages.push([a.num]);
			} else {
				for (i = 0; i < stageData[a.sid].lineages.length; i++) {
					last = stageData[a.sid].lineages[i].length - 1;
					// add transform only to lineage that leads to a
					for (c = 0; c < a.child.length; c++)
						if (a.child[c].num == stageData[a.sid].lineages[i][last]) {
							stageData[a.sid].lineages[i].push(a.num);
							break;
						}
				}
				// If wide dependency add a fromStageRam lineage to next stage
				if (a.dependency == 'wide') {
					type = 'fromSTAGERAM';
					stageData[a.sid + 1].lineages.push([a.num]);
				}
			}
			a.inMemory = a.persistent;

			// si c'est une requete vers hdfs, déterminer le mapping des blocks ici
			if (a.transform == 'textFile') {
				var file = a.args[0];
				var u = url.parse(file);
				if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
					hdfs(worker, u.path, function(blocks) {
						// console.log(blocks);
						// Set node data to be transmitted to workers
						for (var i = 0; i < worker.length; i++) {
							job[i].node[a.num] = a.getArgs(i);
							job[i].node[a.num].args.push(blocks[i]);
							job[i].node[a.num].id = a.id;
							job[i].node[a.num].type = type || a.transform;
							job[i].node[a.num].dependency = a.dependency;
							job[i].node[a.num].persistent = a.persistent;
						}
						callback();
					});
				} else {
					// NFS
					// in this mode files are accessible by workers and master
					var size = fs.statSync(file).size;
					var base = Math.floor(size / worker.length);
					var blocks = [];
					for (i = 0; i < worker.length; i++) {
						blocks[i] = [];
						if (i == worker.length - 1)
							nBytes = size - base * (worker.length - 1);
						else
							nBytes = base;
						var opt = {start: i * base, end: i * base + nBytes - 1};
						blocks[i][0] = {file: file, opt: opt};
						blocks[i][0].skipFirstLine = (i === 0) ? false : true;
				 		blocks[i][0].shuffleLastLine = (i == worker.length - 1) ? false : true;
						blocks[i][0].shuffleTo = i + 1;
						blocks[i][0].bid = i;
					}
					// Set node data to be transmitted to workers
					for (i = 0; i < worker.length; i++) {
						job[i].node[a.num] = a.getArgs(i);
						job[i].node[a.num].args[2] = blocks[i];
						job[i].node[a.num].id = a.id;
						job[i].node[a.num].type = type || a.transform;
						job[i].node[a.num].dependency = a.dependency;
						job[i].node[a.num].persistent = a.persistent;
					}
					callback();
				}
			}
			else {
				// Set node data to be transmitted to workers
				for (i = 0; i < worker.length; i++) {
					job[i].node[a.num] = a.getArgs(i);
					job[i].node[a.num].id = a.id;
					job[i].node[a.num].type = type || a.transform;
					job[i].node[a.num].dependency = a.dependency;
					job[i].node[a.num].persistent = a.persistent;
				}
				callback();
			}
		}));

		var finalStageData = Object.keys(stageData).map(function (i) {return stageData[i];});
		// finalStageData[finalStageData.length - 1].action = action;

		for (i = 0; i < worker.length; i++) {
			job[i].worker = worker;
			job[i].stageData = finalStageData;
			job[i].actionData = action;
		}

		callback(job, streamUgridArrays);
	}).catch(console.log);
}

function hdfs(worker, file, callback) {
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
					if (mapping[worker[i].ip] === undefined)
						mapping[worker[i].ip] = {};
					mapping[worker[i].ip][i] = [];
				}

				// map each block to the least busy worker on same host
				for (i = 0; i < blocks.length; i++) {
					min_id = undefined;
					// boucle sur les hosts du block
					for (var j = 0; j < blocks[i].host.length; j++)
						for (var w in mapping[blocks[i].host[j]]) {
							if (min_id === undefined) {
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
				// var result = worker.map(function() {return []});
				var result = [];
				// callback(mapping[worker[wid].ip][wid]);
				for (var ip in mapping) {
					for (var wid in mapping[ip]) {
						result[wid] = mapping[ip][wid];
					}
				}
				callback(result);
			});
		});
	}).connect({
		host: host,
		username: username,
		privateKey: fs.readFileSync(privateKey)
	});
}
