'use strict';

var util = require('util');
var stream = require('stream');
var trace = require('line-trace');

//var thenify = require('thenify');
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
	arg.data.query = {type: 'worker-controller'};
	UgridClient.call(this, arg);
	var inBrowser = (typeof window != 'undefined');
	var streamId = 0;
	var appid = process.env.appid;
	var workerControllers;
	var maxWorkers = 0;
	var workerPerHost = process.env.UGRID_WORKER_PER_HOST;
	var nWorker = 0;
	var wph, nw = 0;

	this.started = this.ended = false;
	this.jobId = 0;
	this.jobs = {};

	//this.init = thenify.withCallback(function (opt, callback) {

	this.on('connect', function(data) {
		var i;
		workerControllers = data.devices;

		self.on('notify', function (msg) {
			self.worker.push(new Worker(msg.data));
			self.started = true;
			if (++nWorker == maxWorkers)
				callback(null, self);
		});

		for (i = 0; i < workerControllers.length; i++) {
			wph = workerPerHost ? workerPerHost : workerControllers[i].data.ncpu;
			maxWorkers += wph;
			self.send(workerControllers[i].uuid, {
				cmd: 'getWorker',
				appid: data.uuid,
				n: wph
			});
		}

		if (!inBrowser) process.on('exit', this.end);
	});

	function Worker(w) {
		this.uuid = w.uuid;
		this.id = w.id;
		this.ip = w.ip;
	}

	Worker.prototype.rpc = function (cmd, args, callback) {
		self.request({uuid: this.uuid, id: this.id}, {cmd: cmd, args: args}, callback);
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
			//delete self.jobs[msg.data];
		}
	});

	this.end = function () {
		if (self.ended) return;
		self.ended = true;
		for (var i = 0; i < self.worker.length; i++)
			self.worker[i].send("reset");
		if (this.started) self.set({complete: 1});
		self._end();
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

		var a = new UgridArray(self, worker, [], 'narrow', 'randomSVMData', [data]);
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = [];
			data[wid].push(partitions[p]);
			wid = (wid + 1) % worker.length;
		}
		a.args = [data];
		a.getArgs = function(wid) {
			// return [D, a.args[0][wid]];
			return {args: [D, a.args[0][wid]]};
		};
		return a;
	};

	this.parallelize = function (localArray, nPartitions) {
		var a = new UgridArray(self, worker, [], 'narrow', 'parallelize', []);
		a.getArgs = function(w) {
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
			// Map partitions to workers
			for (var p = 0; p < P; p++) {
				if (data[wid] === undefined) data[wid] = [];
				if (partitions[p] === undefined)
					data[wid].push([]);
				else
					data[wid].push(partitions[p]);
				wid = (wid + 1) % worker.length;
			}
			return {args: [data[w]]};
			// return [data[w]];
		};
		return a;
	};

	this.textFile = function (path, nPartitions) {
		return new UgridArray(self, worker, [], 'narrow', 'textFile', [path, nPartitions || worker.length]);
	};

	this.mongo = function (url, query) {
		return new UgridArray(self, worker, [], 'narrow', 'mongo', [url, query]);
	};

	this.stream = function (inputStream, config) {
		var streams = [];
		var n = config.N / worker.length;

		for (var i in worker) {
			streams[i] = self.createWriteStream(streamId, worker[i].uuid);
		}
		var dispatch = new DispatchStream(streams, n, config.N);
		var a =  new UgridArray(self, worker, [], 'narrow', 'stream', [n, streamId++]);
		a.startStream = function () {
			inputStream.pipe(new Lines()).pipe(dispatch);
		};
		return a;
	};
}

function DispatchStream (dests, n, N) {
	if (!(this instanceof DispatchStream))
		return new DispatchStream(dests, n, N);
	stream.Transform.call(this, {objectMode: true});
	this.dests = dests;
	this.n = n;
	this.count = 0;
	this.N = N;
	this.last = 0;
}
util.inherits(DispatchStream, stream.Transform);

DispatchStream.prototype._transform = function (chunk, encoding, done) {
	this.dests[this.last].write(chunk);
	this.count = (this.count + 1) % this.N;
	//this.last = (this.last < this.dests.length - 1) ? this.last + 1 : 0;
	if (this.count % this.n === 0) this.last = (this.last < this.dests.length - 1) ? this.last + 1 : 0;
	done();
};

DispatchStream.prototype._flush = function (done) {
	for (var i in this.dests) {
		this.dests[i].emit('end', (this.count === 0));
	}
	done();
};
