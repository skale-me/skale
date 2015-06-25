'use strict';

var co = require('co');
var thenify = require('thenify');
var Connection = require('ssh2');
var trace = require('line-trace');
var url = require('url');
var fs = require('fs');

var ugridify  = require('./ugridify.js');
var Lines = require('./lines.js');

function UgridArray(grid, child, dependency, transform, args, src) {
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

	this.getArgs = function() {
		return {
			args: JSON.parse(JSON.stringify(this.args)),
			src: src,
			child: child.map(function(c) {return c.id;}),
			transform: this.transform
		};
	};

	this.getNode = function (wid) {
		var node = this.getArgs(wid);
		node.id = this.id;
		node.type = this.transform;
		node.dependency = this.dependency;
		node.persistent = this.persistent;
		node.inMemory = this.inMemory;
		node.sid = (this.dependency == 'wide') ? this.sid + 1 : this.sid;
		node.target_cnt = this.child.length;
		node.stageNode = this.stageNode;
		node.num = this.num;
		return node;
	}

	this.persist = function () {
		this.persistent = true;
		return this;
	};

	this.map = function (mapper, mapperArgs) {
		return new UgridArray(grid, [this], 'narrow', 'map', mapperArgs, mapper.toString());
	};

	this.keys = function () {
		var mapper = function (e) {return e[0];};
		return new UgridArray(grid, [this], 'narrow', 'map', null, mapper.toString());
	};

	this.values = function () {
		var mapper = function (e) {return e[1];};
		return new UgridArray(grid, [this], 'narrow', 'map', null, mapper.toString());
	};

	this.filter = function (filter, filterArgs) {
		return new UgridArray(grid, [this], 'narrow', 'filter', filterArgs, filter.toString());
	};

	this.flatMap = function (mapper, mapperArgs) {
		return new UgridArray(grid, [this], 'narrow', 'flatMap', mapperArgs, mapper.toString());
	};

	this.flatMapValues = function (mapper, mapperArgs) {
		return new UgridArray(grid, [this], 'narrow', 'flatMapValues', mapperArgs, mapper.toString());
	};

	this.sample = function (withReplacement, frac, seed) {
		return new UgridArray(grid, [this], 'wide', 'sample', [withReplacement, frac, seed || 1]);
	};

	this.groupByKey = function () {
		return new UgridArray(grid, [this], 'wide', 'groupByKey', []);
	};

	this.reduceByKey = function (reducer, initVal) {
		return new UgridArray(grid, [this], 'wide', 'reduceByKey', [initVal], reducer.toString());
	};

	this.union = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, [this, other], 'narrow', 'union', [other.id]);
	};

	this.join = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, [this, other], 'wide', 'join', [other.id, 'inner']);
	};

	this.leftOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, [this, other], 'wide', 'join', [other.id, 'left']);
	};

	this.rightOuterJoin = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, [this, other], 'wide', 'join', [other.id, 'right']);
	};

	this.coGroup = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, [this, other], 'wide', 'coGroup', [other.id]);
	};

	this.crossProduct = function (other) {
		if (other.id == this.id) return this;
		return new UgridArray(grid, [this, other], 'wide', 'crossProduct', [other.id]);
	};

	this.mapValues = function (mapper, mapperArgs) {
		return new UgridArray(grid, [this], 'narrow', 'mapValues', mapperArgs, mapper.toString());
	};

	this.distinct = function () {
		return new UgridArray(grid, [this], 'wide', 'distinct', []);
	};

	this.sortByKey = function () {
		return new UgridArray(grid, [this], 'wide', 'sortByKey', []);
	};

	this.partitionByKey = function () {
		return new UgridArray(grid, [this], 'wide', 'partitionByKey', []);
	};

	this.intersection = function (other) {
		return new UgridArray(grid, [this, other], 'wide', 'intersection', []);
	};

	this.subtract = function (other) {
		return new UgridArray(grid, [this, other], 'wide', 'subtract', []);
	};

	this.collect = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		var action = {fun: 'collect', args: [], init: {}};
		var init = [];
		var reducer = function(a, b) {
			for (var p in b) a = a.concat(b[p]);
			return a;
		}
		return buildJob(grid, opt, this, action, callback, init, reducer);
	});

	this.count = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 2) callback = opt;
		var action = {fun: 'count', args: [], init: 0};
		var init = 0;
		var reducer = function(a, b) {return a + b;}
		return buildJob(grid, opt, this, action, callback, init, reducer);
	});

	this.reduce = ugridify(function (reducer, aInit, opt, callback) {
		opt = opt || {};
		if (arguments.length < 4) callback = opt;
		var action = {fun: 'reduce', args: [aInit], src: reducer.toString(), init: aInit};
		return buildJob(grid, opt, this, action, callback, aInit, reducer);
	});

	this.take = ugridify(function (num, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var action = {fun: 'take', args: [num], init: 0};
		var init = [];
		var reducer = function(a, b) {
			for (var i = 0; i < b.length; i++)
				if (a.length < num) a.push(b[i]);
				else break;
			return a;
		}
		return buildJob(grid, opt, this, action, callback, init, reducer);
	});

	this.top = ugridify(function (num, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		// By default sort order is on number
		var numsort = function (a, b) {return b - a;};
		var action = {fun: 'top', args: [num, numsort], init: 0};
		var init = [];
		var reducer = function(a, b) {
			a = a.concat(b);
			a = a.sort(numsort);
			a = a.slice(0, num);
			return a;
		}
		return buildJob(grid, opt, this, action, callback, init, reducer);
	});

	this.takeOrdered = ugridify(function (num, ordering, opt, callback) {
		opt = opt || {};
		if (arguments.length < 4) callback = opt;
		// By default sort order is on number
		//var numsort = function (a, b) {return a - b;};
		var action = {fun: 'takeOrdered', args: [num], src: ordering.toString(), init: 0};
		var init = [];
		var reducer = function(a, b) {
			//a = a.concat(b);
			//a = a.sort(ordering);
			//a = a.slice(0, num);
			return a.concat(b).sort(ordering).slice(0, num);
		}
		return buildJob(grid, opt, this, action, callback, init, reducer);
	});

	this.takeSample = ugridify(function (withReplacement, num, seed, opt, callback) {
		opt = opt || {};
		if (arguments.length < 5) callback = opt;
		var self = this;
		var action = {fun: 'count', args: [], init: 0};
		var init = 0;
		var reducer = function(a, b) {return a + b;}
		return buildJob(grid, opt, this, action, function(err, result) {
			var a = new UgridArray(grid, [self], 'wide', 'sample', [withReplacement, num / result, seed]);
			a.collect({stream: opt.stream, _stream: stream}, callback);
		}, init, reducer);
	});

	this.lookup = ugridify(function (key, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var action = {fun: 'lookup', args: [key], init: {}};
		var init = [];
		var reducer = function(a, b) {
			for (var p in b) a = a.concat(b[p]);
			return a;
		}
		return buildJob(grid, opt, this, action, callback, init, reducer);
	});

	this.countByValue = ugridify(function (opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var action = {fun: 'countByValue', args: [], init: {}};
		var init = {};
		var reducer = function(a, b) {
			for (var i in b) {
				if (a[i] === undefined) a[i] = b[i];
				else a[i][1] += b[i][1];
			}
			return a;
		}
		var postReducer = function(a) {
			var vect = [];
			for (var i in a) vect.push(a[i]);
			return vect;
		}
		return buildJob(grid, opt, this, action, callback, init, reducer, postReducer);
	});

	this.forEach = ugridify(function (eacher, opt, callback) {
		opt = opt || {};
		if (arguments.length < 3) callback = opt;
		var action = {fun: 'forEach', args: [], src: eacher.toString(), init: {}};
		var init = null;
		var reducer = function(a, b) {return null;}
		return buildJob(grid, opt, this, action, callback, init, reducer);
	});
}

module.exports = UgridArray;

// ------------------------------------------------------------------------ //
// Job builder
// ------------------------------------------------------------------------ //
function onError(err) {
	console.error(err.stack);
	process.exit(1);
}

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
	}).catch(onError);
});

function buildJob(grid, opt, array, action, callback, initValue, processPartialResult, postProcess) {
	var nStage = 0, lastStage = 0, num = 0, streamUgridArrays = [];
	var worker = grid.worker;
	var job = new Array(worker.length);
	for (var i = 0; i < job.length; i++) job[i] = {node: {}};

	var jobId = grid.jobId++;
	var stream = opt._stream || new grid.createReadStream(jobId);
	grid.jobs[jobId] = {count: 0, stream: stream, finished: false};
	action.jobId = jobId;
	action.target_cnt = 1;
	var stageNode;

	co(function *() {
		var onlyStreams = true;
		// Treewalk 1: Set stage index, node index and stage array
		yield treewalk(array, thenify(function(a, callback) {
			if (a.dependency == 'wide') nStage++;
			if (lastStage < nStage) lastStage = nStage;
			callback();
		}), thenify(function(a, callback) {
			a.sid = nStage;
			a.num = num++;
			if (a.dependency == 'wide') nStage--;
			if (a.transform == 'stream') {
				// add jobId to list of jobs associated to streamId
				if (grid.streams == undefined) grid.streams = {};
				if (grid.streams[a.streamId] == undefined)
					grid.streams[a.streamId] = [];
				if (grid.streams[a.streamId].indexOf(action.jobId) == -1)
					grid.streams[a.streamId].push(action.jobId);
				// add streamId to list of active input streams associated to jobId
				if (grid.jobs[action.jobId].activeInputStreams == undefined)
					grid.jobs[action.jobId].activeInputStreams = [];
				if (grid.jobs[action.jobId].activeInputStreams.indexOf(a.streamId) == -1)
					grid.jobs[action.jobId].activeInputStreams.push(a.streamId);
				streamUgridArrays.push(a);
			}
			if ((a.child.length == 0) && (a.transform != 'stream'))
				onlyStreams = false;
			callback();
		}));

		/* Set stageNode of each node 
			le noeud action peut devoir attendre plus d'1 seul lineageEnd s'il posséde des transformations narrow 
			possédant plusieurs fils dans sa descandance
			pendant la remonté il faut modifier le stageNode dynamiquement afin de gérer les
			différentes combinaisons de ramification
		*/		
		yield treewalk(array, thenify(function(a, callback) {
			a.stageNode = stageNode;
			if (a.dependency == 'wide') stageNode = a.num;
			callback();
		}), thenify(function(a, callback) {
			if ((stageNode == undefined) && (a.transform == 'union')){
				if (action.target_cnt == 1) action.target_cnt = 2;
				else action.target_cnt += 2;
			}
			callback();
		}));

		var stageData = {};
		for (i = 0; i < lastStage + 1; i++)
			stageData[i] = [];

		// Treewalk 2: Reverse stage index, identify lineages and set persistency
		yield treewalk(array, null, thenify(function(a, callback) {
			var c, i, last, nBytes;

			a.sid = lastStage - a.sid;		// Reverse stage index

			if (a.inMemory) {
				if (a.dependency == 'wide') {
					for (i = 0; i <= a.sid; i++) delete stageData[i];
				} else {
					// if narrow dependency delete current stage lineages that lead to this transform
					// and add a fromRAM to this stage
					var lineagesToBeRemoved = [];
					for (i = 0; i < stageData[a.sid].length; i++) {
						last = stageData[a.sid][i].length - 1;
						for (c = 0; c < a.child.length; c++)
							if (a.child[c].num == stageData[a.sid][i][last]) {
								lineagesToBeRemoved.push(i);
								break;
							}
					}
					for (i = 0; i < lineagesToBeRemoved.length; i++)
						stageData[a.sid].splice(lineagesToBeRemoved[i], 1);
				}
				var sid = (a.dependency == 'wide') ? a.sid + 1 : a.sid;
				stageData[sid].push([a.num]);
			} else if (a.child.length === 0) {
				// If transform is a leaf, add a new lineage to current stage
				stageData[a.sid].push([a.num]);
			} else {
				for (i = 0; i < stageData[a.sid].length; i++) {
					last = stageData[a.sid][i].length - 1;
					// add transform only to lineage that leads to a
					for (c = 0; c < a.child.length; c++)
						if (a.child[c].num == stageData[a.sid][i][last]) {
							stageData[a.sid][i].push(a.num);
							break;
						}
				}
				// If wide dependency add a fromStageRam lineage to next stage
				if (a.dependency == 'wide')
					stageData[a.sid + 1].push([a.num]);
			}

			// si c'est une requete vers hdfs, déterminer le mapping des blocks ici
			if (a.transform == 'textFile') {
				var file = a.args[0];
				var u = url.parse(file);
				if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
					hdfs(worker, u.path, function(blocks) {
						// Set node data to be transmitted to workers
						for (var i = 0; i < worker.length; i++) {
							job[i].node[a.num] = a.getNode(i);
							job[i].node[a.num].args.push(blocks[i]);	// getNode doit etre asynchrone ici
						}
						a.inMemory = a.persistent;
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
						job[i].node[a.num] = a.getNode(i);
						job[i].node[a.num].args[2] = blocks[i];
					}
					a.inMemory = a.persistent;					
					callback();
				}
			}
			else {
				for (i = 0; i < worker.length; i++)
					job[i].node[a.num] = a.getNode(i);
				a.inMemory = a.persistent;				
				callback();
			}
		}));

		var finalStageData = Object.keys(stageData).map(function (i) {return stageData[i];});

		for (i = 0; i < worker.length; i++) {
			job[i].worker = worker;
			job[i].stageData = finalStageData;
			job[i].actionData = action;
		}

		////////////////////////////////////////////////////////////
		// construction du node_path à faire côté worker
		////////////////////////////////////////////////////////////
		for (var sid = 0; sid < finalStageData.length; sid++) {
			var isLastStage = (sid == finalStageData.length - 1);
			for (var lid = 0; lid < finalStageData[sid].length; lid++) {
				job.map(function(jw) {
					jw.node[finalStageData[sid][lid][0]].node_path = finalStageData[sid][lid];
					jw.node[finalStageData[sid][lid][0]].inLastStage = isLastStage;
				});
			}
		}

		grid.jobs[action.jobId].onlyStreams = onlyStreams;
		grid.jobs[action.jobId].iteration = 0;

		// deploy job
		var nAnswer = 0, dones = [];
		var mainResult = JSON.parse(JSON.stringify(initValue));
		sendJob(job, jobId, streamUgridArrays, function (err, res, done) {
			dones.push(done);
			mainResult = processPartialResult(mainResult, res);
			if (++nAnswer < worker.length) return;
			grid.jobs[jobId].iteration++;
			if (postProcess) mainResult = postProcess(mainResult);
			if (!grid.jobs[jobId].ignore) {
				if (opt.stream) {
					stream.write(mainResult, function () {
						for (var i in dones) dones[i]();
						dones = [];
					});
				} else {
					callback(null, mainResult);
					for (var i in dones) dones[i]();
					dones = [];
				}
			}
			mainResult = JSON.parse(JSON.stringify(initValue));
			nAnswer = 0;
			// unlock toWorkers input streams
			for (var s in streamUgridArrays)
				try {streamUgridArrays[s].dispatch.done();} catch (e) {};
		});		
		// runJob();
		// callback(job, jobId, streamUgridArrays, stream);		
	}).catch(onError);

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
				for (var s in streamUgridArrays)
					streamUgridArrays[s].startStream();
			});
	}

	return stream;
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
				var result = [];
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
