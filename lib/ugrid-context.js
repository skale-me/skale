'use strict';

var util = require('util');
var stream = require('stream');
//var trace = require('line-trace');
var url = require('url');
var fs = require('fs');

var UgridClient = require('./ugrid-client.js');
var UgridArray = require('./ugrid-array.js');
var Lines = require('./lines.js');
var splitLocalFile = require('../utils/readsplit.js').splitLocalFile;
var splitHDFSFile = require('../utils/readsplit.js').splitHDFSFile;

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
		if (this.started) self.set({complete: 1});
		self._end();
	};

	this.randomSVMData = function (N, D, seed, nPartitions) {
		var a = new UgridArray(self, [], 'randomSVMData', []);
		a.getArgs = function(callback) {
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

			// Map partitions to workers
			for (p = 0; p < P; p++) {
				if (data[wid] === undefined) data[wid] = [];
				data[wid].push(partitions[p]);
				wid = (wid + 1) % worker.length;
			}

			var args = [];
			for (var i = 0; i < worker.length; i++) args.push([D, data[i]]);
			callback(args);
		};
		return a;
	};

	this.parallelize = function (localArray, nPartitions) {
		var a = new UgridArray(self, [], 'parallelize', []);
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
				if (partitions[p] === undefined) data[wid].push([]);
				else data[wid].push(partitions[p]);		// MODIFIED HERE for next implementation
				wid = (wid + 1) % worker.length;
			}
			callback(data);
		};
		return a;
	};

	this.textFile = function (path, nPartitions) {
		var a = new UgridArray(self, [], 'textFile', [path, nPartitions || worker.length]);
		a.getArgs = function(callback) {
			var file = a.args[0];
			var nSplit = nPartitions || worker.length;
			var u = url.parse(file);

			if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) splitHDFSFile(u.path, nSplit, mapLogicalSplit);
			else splitLocalFile(u.path, nSplit, mapLogicalSplit);

			function mapLogicalSplit(split) {
				for (var i = 0; i < split.length; i++) split[i].wid = i % worker.length;
				var args = [];
				for (var i = 0; i < worker.length; i++)
					args.push(JSON.parse(JSON.stringify(a.args)).concat([split]));
				callback(args);
			}
		}
		return a;
	};

	function stream(inputStream, config, streamType, streamid) {
		var a =  new UgridArray(self, [], 'stream', [0, streamid]);

		a.config = config;
		self.streams[streamid] = [];	// job vector to whom stream take part
		a.worker_count = 0;

		a.startStream = function () {
			if (++a.worker_count < worker.length) return;
			if (streamType == 'line') inputStream.pipe(new Lines()).pipe(a.dispatch);
			else inputStream.pipe(a.dispatch);
		};

		a.getArgs = function(callback) {
			var args = [], streams = [], mapN = [];

			for (var i = 0; i < worker.length; i++) {
				args.push([mapN[i], JSON.parse(JSON.stringify(a.args[1]))]);
				streams[i] = self.createWriteStream(streamid, worker[i].uuid);
			}
		    a.dispatch = new DispatchStream(self, streams, streamid, mapN, null);
			a.streamId = streamid;
			callback(args);
		}

		return a;
	}

	this.lineStream = function (inputStream, config) {
		return stream(inputStream, config, 'line', streamId++);
	};

	this.objectStream = function (inputStream, config) {
		return stream(inputStream, config, 'object', streamId++);
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
