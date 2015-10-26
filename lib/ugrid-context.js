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

function UgridContext(arg) {
	if (!(this instanceof UgridContext))
		return new UgridContext(arg);
	var self = this, worker = this.worker = [];

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
	this.firstData = {};
	this.contextId = Date.now();

	this.once('connect', function(data) {
		var i;
		if (!maxWorker || maxWorker > data.devices.length)
			maxWorker = data.devices.length;
		for (i = 0; i < maxWorker; i++) {
			self.worker.push(new Worker(data.devices[i]));
			self.firstData[data.devices[i].uuid] = data.devices;
		}
		self.started = true;
		// XXXX: the following line is a bad workaround. Fix mem leaks instead of forcing exit
		if (!inBrowser) process.on('exit', this.end);
	});

	this.on('workerError', function workerError(msg) {
		console.error('Error from worker id %d:', msg.from)
		console.error(msg.args);
	});

	this.on('remoteClose', function getWorkerClose(msg) {
		throw 'Fatal error: unexpected worker exit';
	});

	this.getWorkers = function (callback) {
		if (self.started) return callback();
		this.once('connect', function() {callback();});
	}

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

	this.on('startStream', function (msg) {
		var job = self.jobs[msg.jobid];
		job.streamsToLaunch[msg.streamid].startStream();
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

	// this.textFile = function (path, nPartitions) {
	// 	var a = new UgridArray(self, [], 'narrow', 'textFile', [path, nPartitions || worker.length]);
	// 	a.getArgs = function(callback) {
	// 		var file = a.args[0];
	// 		var u = url.parse(file);
	// 		if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
	// 			hdfs(worker, u.path, function(blocks) {	// HDFS, all file chunks must be accessible by workers
	// 				var args = [];
	// 				for (var i = 0; i < worker.length; i++) args.push([blocks[i]])
	// 				callback(args);
	// 			});
	// 		} else { // NFS (files must be accessible by workers and master)
	// 			var size = fs.statSync(file).size;
	// 			var base = Math.floor(size / worker.length);
	// 			var blocks = [];
	// 			var nBytes;
	// 			for (var i = 0; i < worker.length; i++) {
	// 				blocks[i] = [];
	// 				if (i == worker.length - 1) nBytes = size - base * (worker.length - 1);
	// 				else nBytes = base;
	// 				var opt = {start: i * base, end: i * base + nBytes - 1};
	// 				if (opt.end < opt.start) opt.end = opt.start;
	// 				blocks[i][0] = {file: file, opt: opt};
	// 				blocks[i][0].skipFirstLine = (i === 0) ? false : true;
	// 		 		blocks[i][0].shuffleLastLine = (i == worker.length - 1) ? false : true;
	// 				blocks[i][0].shuffleTo = i + 1;
	// 				blocks[i][0].bid = i;
	// 			}
	// 			var args = [];
	// 			for (var i = 0; i < worker.length; i++) {
	// 				var tmp = JSON.parse(JSON.stringify(a.args));
	// 				args.push(tmp.concat([blocks[i]]))
	// 			}
	// 			callback(args);
	// 		}
	// 	}
	// 	return a;
	// };

	// Version NFS and single node localFS only 
	this.textFile = function (path, nPartitions) {
		var a = new UgridArray(self, [], 'narrow', 'textFile', [path, nPartitions || worker.length]);
		a.getArgs = function(callback) {
			var file = a.args[0];
			var size = fs.statSync(file).size;
			var maxBlockSize = Math.ceil(size / worker.length);		// taille max d'un block (minimum de 1 byte dans un block)
			var start = 0;
			var blocks = [];
			// var str = fs.readFileSync(file, {encoding: 'utf8'});
			// console.log(str + '\n')
			while (start < size) {
				// console.log('\n# block: ' + blocks.length)
				// console.log(str.substr(start, maxBlockSize + 1))
				blocks.push({file: file, opt: {start: start, end: start + maxBlockSize}, bid: blocks.length, wid: blocks.length});
				start += maxBlockSize + 1;
			}
			// console.log('----------------------\n')
			/* si on force la taille minimale des blocks à 2*/

			// Send blocks to workers
			var args = [];
			for (var i = 0; i < worker.length; i++)
				args.push(JSON.parse(JSON.stringify(a.args)).concat([blocks]))
			callback(args);
		}
		return a;
	};

	function stream(inputStream, config, streamType) {
		var streams = [], mapN = [];
		for (var i in worker)
			streams[i] = self.createWriteStream(streamId, worker[i].uuid);
		var a =  new UgridArray(self, [], 'narrow', 'stream', [0, streamId]);
		var dispatch = a.dispatch = new DispatchStream(self, streams, streamId, mapN, null);
		a.config = config;
		self.streams[streamId] = [];	// job vector to whom stream take part
		a.streamId = streamId++;

		a.worker_count = 0;
		a.startStream = function () {
			if (++a.worker_count < worker.length) return;
			if (streamType == 'line') inputStream.pipe(new Lines()).pipe(dispatch);
			else inputStream.pipe(dispatch);
		};

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
	this.last = 0;
}

util.inherits(DispatchStream, stream.Transform);

DispatchStream.prototype._transform = function (chunk, encoding, done) {
	this.dests[this.last].write(chunk);					// write data to next worker
	if (++this.last == this.dests.length) this.last = 0;		// rotate worker idx for next write
	done();													// call done
};

DispatchStream.prototype._flush = function (done) {	
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
