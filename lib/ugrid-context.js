'use strict';

var util = require('util');
var stream = require('stream');
var trace = require('line-trace');

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

		var a = new UgridArray(self, [], 'narrow', 'randomSVMData', [data]);
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = [];
			data[wid].push(partitions[p]);
			wid = (wid + 1) % worker.length;
		}
		a.args = [data];
		a.getArgs = function(wid) {
			return {args: [D, a.args[0][wid]]};
		};
		return a;
	};

	this.parallelize = function (localArray, nPartitions) {
		var a = new UgridArray(self, [], 'narrow', 'parallelize', []);
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
		};
		return a;
	};

	this.textFile = function (path, nPartitions) {
		return new UgridArray(self, [], 'narrow', 'textFile', [path, nPartitions || worker.length]);
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

		a.getArgs = function(w) {
			return {args: [mapN[w], a.args[1]]};
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
		// ----------------------------------------------------------------------------------- //
		// quand un block est complet et si (N < worker.length) on evoie la commande streamId.block
		// à tous les workers restants afin de débloquer le lineage distant correspondant,
		if ((this.last > 0) && (this.N < this.grid.worker.length))
			for (var i = this.last; i < this.grid.worker.length; i++)
				this.grid.request(this.grid.worker[i], {cmd: 'block', streamId: streamId}, function() {});
		// ----------------------------------------------------------------------------------- //
		this.last = 0;
		this.done = done;
	} else done();
};

DispatchStream.prototype._flush = function (done) {
	// quand le stream se termine, on boucle dans tous les jobs l'incluant,
	// si dans chaque job le nombre d'iteration est > 0 et que le stream
	// en question est le dernier en activité, alors si le stream fini
	// sur une frontière de block on positionne le ignore à true.
	// console.log(this.grid.streams)
	var impactedJobs = this.grid.streams[this.streamId];
	for (var i = 0; i < impactedJobs.length; i++) {
		// remove current streamId from activeInputStreams
		var id = this.grid.jobs[impactedJobs[i]].activeInputStreams.indexOf(this.streamId);
		this.grid.jobs[impactedJobs[i]].activeInputStreams.splice(id, 1);
		// si il reste des streams après le splice, on ne fait rien
		if (this.grid.jobs[impactedJobs[i]].activeInputStreams.length) continue;
		// sinon si premiere iteration mais présence d'input de type batch, on ne fait rien
		if ((this.grid.jobs[impactedJobs[i]].iteration == 0) && !this.grid.jobs[impactedJobs[i]].onlyStreams) continue;
		// sinon si fin sur frontière de macro block
		if (this.total == 0)
			this.grid.jobs[impactedJobs[i]].ignore = true;
	}

	for (var i in this.dests) this.dests[i].emit('end');
	done();
};
