'use strict';

var util = require('util');
var stream = require('stream');
var trace = require('line-trace');
var url = require('url');
var fs = require('fs');
var Connection = require('ssh2');

var UgridClient = require('./ugrid-client.js');
var UgridArray = require('./ugrid-array.js');
var Lines = require('./lines.js');

module.exports = UgridContext;

util.inherits(UgridContext, UgridClient);

function UgridContext(arg, callback) {
	if (!(this instanceof UgridContext))
		return new UgridContext(arg, callback);
	var self = this, worker = this.worker = [];
	if (!callback) {
		callback = arg;
		arg = undefined;
	}
	arg = arg || {};
	arg.data = arg.data || {};
	arg.data.type = 'master';
	UgridClient.call(this, arg);
	var inBrowser = (typeof window != 'undefined');
	var streamId = 0;
	var maxWorker = process.env.UGRID_MAX_WORKER;

	this.started = this.ended = false;
	this.jobId = 0;
	this.jobs = {};
	this.streams = {};

	this.on('connect', function(data) {
		var i;
		if (!maxWorker || maxWorker > data.devices.length)
			maxWorker = data.devices.length;
		for (i = 0; i < maxWorker; i++) {
			self.worker.push(new Worker(data.devices[i]));
		}
		self.started = true;
		callback(null, self);
		if (!inBrowser) process.on('exit', this.end);
	});

	function Worker(w) {
		this.uuid = w.uuid;
		this.id = w.id;
		this.ip = w.ip;
	}

	Worker.prototype.rpc = function (cmd, args, done) {
		self.request({uuid: this.uuid, id: this.id}, {cmd: cmd, args: args}, done);
	};

	Worker.prototype.send = function (cmd, args) {
		self.send(this.uuid, {cmd: cmd, args: args});
	};

	this.on('request', function (msg) {
		// Protocol to handle stream flow control: reply when data is consumed
		if (msg.data.cmd == 'stream') {
			self.emit(msg.data.stream, msg.data.data, function() {
				try {self.reply(msg);} catch(err) {}
			});
		}
	});

	this.on('endJob', function (msg) {
		var job = self.jobs[msg.data];
		if (job && ++job.count == worker.length) {
			job.stream.emit('end');
		}
	});

	this.end = function () {
		if (self.ended) return;
		self.ended = true;
		if (this.started)
			self.set({complete: 1});
		self._end();
	};

	this.randomSVMData = function (N, D, seed, nPartitions) {
		// by default number of partitions equals number of workers
		var i, p, partitions = [], data = [], acc, wid = 0;
		var P = Math.max(nPartitions || worker.length, worker.length);

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

		var a = new UgridArray(self, [], 'narrow', 'randomSVMData', [data]);
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = [];
			data[wid].push(partitions[p]);
			wid = (wid + 1) % worker.length;
		}
		a.args = [data];
		a.getArgs = function(callback) {
			var args = [];
			for (var i = 0; i < worker.length; i++)
				args.push([D, a.args[0][i]]);
			callback(args);
		};
		return a;
	};

	this.parallelize = function (localArray, nPartitions) {
		var a = new UgridArray(self, [], 'narrow', 'parallelize', []);
		a.getArgs = function(callback) {
			var data = [], wid = 0;
			var P = nPartitions || worker.length;

			function split(a, n) {
				var len = a.length, out = [], i = 0;
				while (i < len) {
					var size = Math.ceil((len - i) / n--);
					out.push(a.slice(i, i += size));
				}
				return out;
			}
			var partitions = split(localArray, P);
			for (var p = 0; p < P; p++) {
				if (data[wid] === undefined) data[wid] = [];
				if (partitions[p] === undefined)
					data[wid].push([]);
				else
					data[wid].push(partitions[p]);		// MODIFIED HERE for nex implementation
				wid = (wid + 1) % worker.length;
			}
			callback(data);
		};
		return a;
	};

	this.textFile = function (path, nPartitions) {
		var a = new UgridArray(self, [], 'narrow', 'textFile', [path, nPartitions || worker.length]);
		a.getArgs = function(callback) {
			var file = a.args[0];
			var u = url.parse(file);
			if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
				hdfs(worker, u.path, function(blocks) {	// HDFS, all file chunks must be accessible by workers
					var args = [];
					for (var i = 0; i < worker.length; i++) args.push([blocks[i]])
					callback(args);
				});
			} else { // NFS (files must be accessible by workers and master)
				var size = fs.statSync(file).size;
				var base = Math.floor(size / worker.length);
				var blocks = [];
				var nBytes;
				for (var i = 0; i < worker.length; i++) {
					blocks[i] = [];
					if (i == worker.length - 1) nBytes = size - base * (worker.length - 1);
					else nBytes = base;
					var opt = {start: i * base, end: i * base + nBytes - 1};
					blocks[i][0] = {file: file, opt: opt};
					blocks[i][0].skipFirstLine = (i === 0) ? false : true;
			 		blocks[i][0].shuffleLastLine = (i == worker.length - 1) ? false : true;
					blocks[i][0].shuffleTo = i + 1;
					blocks[i][0].bid = i;
				}
				var args = [];
				for (var i = 0; i < worker.length; i++) {
					var tmp = JSON.parse(JSON.stringify(a.args));
					args.push(tmp.concat([blocks[i]]))
				}
				callback(args);
			}
		}
		return a;
	};

	function stream(inputStream, config, streamType) {
		var streams = [], mapN = [];
		var n = (config.N <= worker.length) ? 1 : Math.floor(config.N / worker.length);

		for (var i = 0; i < worker.length; i++) mapN[i] = n;
		for (var i = 0; i < config.N - worker.length * n; i++) mapN[i]++;
		for (var i in worker)
			streams[i] = self.createWriteStream(streamId, worker[i].uuid);
		var a =  new UgridArray(self, [], 'narrow', 'stream', [n, streamId]);
		var dispatch = a.dispatch = new DispatchStream(self, streams, streamId, mapN, config.N);
		a.config = config;
		self.streams[streamId] = [];	// job vector to whom stream take part
		a.streamId = streamId++;
		if (streamType == 'line') {
			a.startStream = function () {
				inputStream.pipe(new Lines()).pipe(dispatch);
			};
		} else {
			a.startStream = function () {
				inputStream.pipe(dispatch);
			};
		}

		a.getArgs = function(callback) {
			var args = [];
			for (var i = 0; i < worker.length; i++)
				args.push([mapN[i], JSON.parse(JSON.stringify(a.args[1]))]);
			callback(args);
		}

		return a;
	}

	this.lineStream = function (inputStream, config) {
		return stream(inputStream, config, 'line');
	};

	this.objectStream = function (inputStream, config) {
		return stream(inputStream, config, 'object');
	};
}

function DispatchStream (grid, dests, streamId, mapN, N) {
	if (!(this instanceof DispatchStream))
		return new DispatchStream(dests, mapN, N);
	stream.Transform.call(this, {objectMode: true});
	this.dests = dests;
	this.mapN = mapN;
	this.countPerWorker = mapN.map(function() {return 0});
	this.total = 0;
	this.N = N;
	this.last = 0;
	this.grid = grid;
	this.streamId = streamId;
}

util.inherits(DispatchStream, stream.Transform);

DispatchStream.prototype._transform = function (chunk, encoding, done) {
	var streamId = this.streamId;
	this.dests[this.last].write(chunk);
	this.countPerWorker[this.last] = (this.countPerWorker[this.last] + 1) % this.mapN[this.last];
	this.total = (this.total + 1) % this.N;

	if (this.countPerWorker[this.last] === 0)
		this.last = (this.last < this.dests.length - 1) ? this.last + 1 : 0;

	if (this.total === 0) {
		// quand un block est complet et si (N < worker.length) on evoie la commande streamId.block
		// à tous les workers restants afin de débloquer le lineage distant correspondant,
		if ((this.last > 0) && (this.N < this.grid.worker.length))
			for (var i = this.last; i < this.grid.worker.length; i++)
				this.grid.request(this.grid.worker[i], {cmd: 'block', streamId: streamId}, function() {});
		this.last = 0;
		this.done = done;
	} else done();
};

DispatchStream.prototype._flush = function (done) {
	var jobs = this.grid.streams[this.streamId];
	for (var i = 0; i < jobs.length; i++) {
		// remove current streamId from activeInputStreams
		var id = this.grid.jobs[jobs[i]].activeInputStreams.indexOf(this.streamId);
		this.grid.jobs[jobs[i]].activeInputStreams.splice(id, 1);
		// si il reste des streams après le splice, on ne fait rien
		if (this.grid.jobs[jobs[i]].activeInputStreams.length) continue;
		// sinon si premiere iteration mais présence d'input de type batch, on ne fait rien
		if ((this.grid.jobs[jobs[i]].iteration == 0) && !this.grid.jobs[jobs[i]].onlyStreams) continue;
		// sinon si fin sur frontière de macro block
		if (this.total == 0)
			this.grid.jobs[jobs[i]].ignore = true;
	}

	for (var i in this.dests) this.dests[i].emit('end');
	done();
};

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