'use strict';

var co = require('co');
var thenify = require('thenify');
var Connection = require('ssh2');
var url = require('url');
var fs = require('fs');

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

	function rpc(grid, cmd, workerNum, args, callback) {
		grid.request(worker[workerNum], {cmd: cmd, args: args, master_uuid: grid.uuid}, callback);
	}

	function sendJob(job, callback) {
		// Launch Streams on master side as event listener on worker side are engaged
		setInterval(function() {
			grid.send(worker[0].uuid, {cmd: 'unique_stream_id', data: ['hello world']}, function(err) {if (err) throw new Error(err);});
		}, 1000)

		grid.on('0', function(msg) {callback(null, msg.result)});

		var n = 0;
		for (var i = 0; i < worker.length; i++)
			rpc(grid, 'setJob', i, job[i], function () {
				if (++n < worker.length) return;
				// Ici on récupere les uuid des streamer distants
				// on peut alors initier la séquence de stream coté master
				// puis indiquer aux workers de démarrer le job, ceux ci
				// positionnent alors
				// when all workers ready, start job
				for (var j = 0; j < worker.length; j++)
					grid.send(worker[j].uuid, {cmd: 'runJob'})
			});
	}

	this.getArgs = function() {
		return {
			args: JSON.parse(JSON.stringify(this.args)),
			src: src,
			child: child.map(function(c) {return c.id;}),
			transform: this.transform
		};
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, mapperArgs) {
		return new UgridArray(grid, worker, [this], 'narrow', 'map', mapperArgs, mapper.toString());
	};

	this.keys = function () {
		var mapper = function (e) {return e[0];}
		return new UgridArray(grid, worker, [this], 'narrow', 'map', null, mapper.toString());
	};

	this.values = function () {
		var mapper = function (e) {return e[1];}
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

	this.reduce = thenify(function (reducer, aInit, callback) {
		buildJob(worker, this, {fun: 'reduce', args: [aInit], src: reducer.toString(), init: aInit}, function (job) {
			var nAnswer = 0;
			sendJob(job, function (err, res) {
				aInit = reducer(aInit, res);
				if (++nAnswer == worker.length) {
					callback(null, aInit);
					nAnswer = 0;
				}
			});
		});
	});

	this.take = thenify(function (num, callback) {
		buildJob(worker, this, {fun: 'take', args: [num], init: 0}, function (job) {
			var nAnswer = 0, result = [];
			sendJob(job, function (err, res) {
				for (var i = 0; i < res.length; i++)
					if (result.length < num) result.push(res[i]);
					else break;
				if (++nAnswer == worker.length) {
					callback(null, result);
					nAnswer = 0;
					result = [];
				}
			});
		});
	});

	this.top = thenify(function (num, callback) {
		// By default sort order is on number
		var numsort = function (a, b) {return b - a;}
		buildJob(worker, this, {fun: 'top', args: [num, numsort], init: 0}, function (job) {
			var nAnswer = 0, result = [];
			sendJob(job, function (err, res) {
				result = result.concat(res);
				result = result.sort(numsort);
				result = result.slice(0, num);
				if (++nAnswer == worker.length) {
					callback(null, result);
					nAnswer = 0;
					result = [];
				}
			});
		});
	});

	this.takeOrdered = thenify(function (num, callback) {
		// By default sort order is on number
		var numsort = function (a, b) {return a - b;}
		buildJob(worker, this, {fun: 'takeOrdered', args: [num, numsort], init: 0}, function (job) {
			var nAnswer = 0, result = [];
			sendJob(job, function (err, res) {
				result = result.concat(res);
				result = result.sort(numsort);
				result = result.slice(0, num);
				if (++nAnswer == worker.length) {
					callback(null, result);
					nAnswer = 0;
					result = [];
				}
			});
		});
	});

	this.takeSample = thenify(function (withReplacement, num, seed, callback) {
		var self = this;
		buildJob(worker, this, {fun: 'count', args: [], init: 0}, function (job) {
			var nAnswer = 0, result = 0;
			sendJob(job, function (err, res) {
				result += res;
				if (++nAnswer == worker.length) {
					var a = new UgridArray(grid, worker, [self], 'wide', 'sample', [withReplacement, num / result, seed]);
					a.collect(callback);
					nAnswer = 0;
					result = 0;
				}
			});
		});
	});

	this.count = thenify(function (callback) {
		buildJob(worker, this, {fun: 'count', args: [], init: 0}, function (job) {
			var nAnswer = 0, result = 0;
			sendJob(job, function (err, res) {
				result += res;
				if (++nAnswer == worker.length) {
					callback(null, result);
					nAnswer = 0;
					result = 0;
				}
			});
		});
	});

	// NB Reporter le reset de l'action dans toutes les actions
	this.collect = thenify(function (callback) {
		buildJob(worker, this, {fun: 'collect', args: [], init: {}}, function (job) {
			var nAnswer = 0, vect = [];
			sendJob(job, function (err, res) {
				for (var p in res) vect = vect.concat(res[p]);
				if (++nAnswer == worker.length) {
					callback(null, vect);
					vect = [];
					nAnswer = 0;
				}
			});
		});
	});

	this.lookup = thenify(function (key, callback) {
		buildJob(worker, this, {fun: 'lookup', args: [key], init: {} }, function (job) {
			var nAnswer = 0, vect = [];
			sendJob(job, function (err, res) {
				for (var p in res) vect = vect.concat(res[p]);
				if (++nAnswer == worker.length) {
					callback(null, vect);
					vect = [];
					nAnswer = 0;
				}
			});
		});
	});

	this.countByValue = thenify(function (callback) {
		buildJob(worker, this, {fun: 'countByValue', args: [], init: {}}, function (job) {
			var nAnswer = 0, tmp = {};
			sendJob(job, function (err, res) {
				for (var i in res) {
					if (tmp[i] == undefined) tmp[i] = res[i];
					else tmp[i][1] += res[i][1];
				}
				if (++nAnswer == worker.length) {
					var vect = [];
					for (var i in tmp) vect.push(tmp[i]);
					callback(null, vect);
					nAnswer = 0;
					tmp = {};
				}
			});
		});
	});

	this.forEach = thenify(function (eacher, callback) {
		buildJob(worker, this, {fun: 'forEach', args: [], src: eacher.toString(), init: {}}, function (job) {
			var nAnswer = 0;
			sendJob(job, function (err, res) {
				if (++nAnswer == worker.length) {
					callback(null, null);
					nAnswer = 0;
				}
			});
		});
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
	var nStage = 0, lastStage = 0, num = 0;
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
			callback();
		}));
		var stageData = {};
		for (i = 0; i < lastStage + 1; i++)
			stageData[i] = {lineages: []};

		// Treewalk 2: Reverse stage index, identify lineages and set persistency
		yield treewalk(array, null, thenify(function(a, callback) {
			// Reverse stage index
			a.sid = lastStage - a.sid;
			if (a.dependency == 'wide') {
				stageData[a.sid].shuffleType = a.transform;
				stageData[a.sid].shuffleNum = a.num;
			}

			if (a.inMemory) {
				// if wide dependency discard stage up to this one and add a fromRAM to next stage
				if (a.dependency == 'wide') {
					for (var i = 0; i <= a.sid; i++) delete stageData[i];
				} else {
					// if narrow dependency delete current stage lineages that lead to this transform
					// and add a fromRAM to this stage
					var lineagesToBeRemoved = [];
					for (var i = 0; i < stageData[a.sid].lineages.length; i++) {
						var last = stageData[a.sid].lineages[i].length - 1;
						for (var c = 0; c < a.child.length; c++)
							if (a.child[c].num == stageData[a.sid].lineages[i][last]) {
								lineagesToBeRemoved.push(i);
								break;
							}
					}
					for (var i = 0; i < lineagesToBeRemoved.length; i++)
						stageData[a.sid].lineages.splice(lineagesToBeRemoved[i], 1);
				}
				var sid = (a.dependency == 'wide') ? a.sid + 1 : a.sid;
				var type = 'fromRAM';
				stageData[sid].lineages.push([a.num]);
			} else if (a.child.length === 0) {
				// If transform is a leaf, add a new lineage to current stage
				stageData[a.sid].lineages.push([a.num]);
			} else {
				for (i = 0; i < stageData[a.sid].lineages.length; i++) {
					var last = stageData[a.sid].lineages[i].length - 1;
					// add transform only to lineage that leads to a
					for (var c = 0; c < a.child.length; c++)
						if (a.child[c].num == stageData[a.sid].lineages[i][last]) {
							stageData[a.sid].lineages[i].push(a.num);
							break;
						}
				}
				// If wide dependency add a fromStageRam lineage to next stage
				if (a.dependency == 'wide') {
					var type = 'fromSTAGERAM';
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
					})
				} else {
					// NFS
					// in this mode files are accessible by workers and master
					var size = fs.statSync(file).size;
					var base = Math.floor(size / worker.length);
					var blocks = [];
					for (var i = 0; i < worker.length; i++) {
						blocks[i] = [];
						if (i == worker.length - 1)
							var nBytes = size - base * (worker.length - 1);
						else
							var nBytes = base;
						var opt = {start: i * base, end: i * base + nBytes - 1};
						blocks[i][0] = {file: file, opt: opt};
				 		blocks[i][0].skipFirstLine = (i == 0) ? false : true;
				 		blocks[i][0].shuffleLastLine = (i == worker.length - 1) ? false : true;
						blocks[i][0].shuffleTo = i + 1;
						blocks[i][0].bid = i;
					}
					// Set node data to be transmitted to workers
					for (var i = 0; i < worker.length; i++) {
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
				for (var i = 0; i < worker.length; i++) {
					job[i].node[a.num] = a.getArgs(i);
					job[i].node[a.num].id = a.id;
					job[i].node[a.num].type = type || a.transform;
					job[i].node[a.num].dependency = a.dependency;
					job[i].node[a.num].persistent = a.persistent;
				}
				callback();
			}
		}));

		var finalStageData = Object.keys(stageData).map(function (i) {return stageData[i]});
		finalStageData[finalStageData.length - 1].action = action;

		for (var i = 0; i < worker.length; i++) {
			job[i].worker = worker;
			job[i].stageData = finalStageData;
		}
		
		callback(job);		
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