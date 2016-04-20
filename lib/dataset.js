// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

'use strict';

var fs = require('fs'), url = require('url'), path = require('path');
var util = require('util'), stream = require('stream');
var uuid = require('node-uuid');
var splitLocalFile = require('./readsplit.js').splitLocalFile;
var splitHDFSFile = require('./readsplit.js').splitHDFSFile;

function Dataset(sc, dependencies) {
	this.id = sc.datasetIdCounter++;
	this.dependencies = dependencies || [];
	this.persistent = false;
	this.sc = sc;
}

Dataset.prototype.persist = function () {this.persistent = true; return this;};

Dataset.prototype.map = function (mapper, args) {return new Map(this, mapper, args);};

Dataset.prototype.flatMap = function (mapper, args) {return new FlatMap(this, mapper, args);};

Dataset.prototype.mapValues = function (mapper, args) {return new MapValues(this, mapper, args);};

Dataset.prototype.flatMapValues = function (mapper, args) {return new FlatMapValues(this, mapper, args);};

Dataset.prototype.filter = function (filter, args) {return new Filter(this, filter, args);};

Dataset.prototype.sample = function (withReplacement, frac, seed) {return new Sample(this, withReplacement, frac, seed || 1);};

Dataset.prototype.union = function (other) {return (other.id == this.id) ? this : new Union(this.sc, [this, other]);};

Dataset.prototype.aggregateByKey = function(reducer, combiner, init, args) {
	if (arguments.length < 3) throw new Error('Missing argument for function aggregateByKey().');
	return new AggregateByKey(this.sc, [this], reducer, combiner, init, args);
};

Dataset.prototype.reduceByKey = function (reducer, init, args) {
	if (arguments.length < 2) throw new Error('Missing argument for function reduceByKey().');
	return new AggregateByKey(this.sc, [this], reducer, reducer, init, args);
};

Dataset.prototype.groupByKey = function () {
	function reducer(a, b) {a.push(b); return a;}
	function combiner(a, b) {return a.concat(b);}
	return new AggregateByKey(this.sc, [this], reducer, combiner, [], {});
};

Dataset.prototype.coGroup = function (other) {
	function reducer(a, b) {a.push(b); return a;}
	function combiner(a, b) {
		for (var i = 0; i < b.length; i++) a[i] = a[i].concat(b[i]);
		return a;
	}
	return new AggregateByKey(this.sc, [this, other], reducer, combiner, [], {});
};

Dataset.prototype.cartesian = function (other) {return new Cartesian(this.sc, [this, other]);};

Dataset.prototype.sortBy = function (sorter, ascending, numPartitions) {
	return new SortBy(this.sc, this, sorter, ascending, numPartitions);
};

Dataset.prototype.partitionBy = function (partitioner) {
	return new PartitionBy(this.sc, this, partitioner);
};

Dataset.prototype.sortByKey = function (ascending, numPartitions) {
	return new SortBy(this.sc, this, function(data) {return data[0];}, ascending, numPartitions);
};

Dataset.prototype.join = function (other) {
	return this.coGroup(other).flatMapValues(function(v) {
		var res = [];
		for (var i in v[0])
			for (var j in v[1])
				res.push([v[0][i], v[1][j]]);
		return res;
	});
};

Dataset.prototype.leftOuterJoin = function (other) {
	return this.coGroup(other).flatMapValues(function(v) {
		var res = [], i, j;
		if (v[1].length == 0) {
			for (i in v[0]) res.push([v[0][i], null]);
		} else {
			for (i in v[0])
				for (j in v[1]) res.push([v[0][i], v[1][j]]);
		}
		return res;
	});
};

Dataset.prototype.rightOuterJoin = function (other) {
	return this.coGroup(other).flatMapValues(function(v) {
		var res = [], i, j;
		if (v[0].length == 0) {
			for (i in v[1]) res.push([null, v[1][i]]);
		} else {
			for (i in v[0])
				for (j in v[1]) res.push([v[0][i], v[1][j]]);
		}
		return res;
	});
};

Dataset.prototype.distinct = function () {
	return this.map(e => [e, null]).reduceByKey(a => a, null).map(a => a[0]);
};

Dataset.prototype.intersection = function (other) {
	var a = this.map(e => [e, 0]).reduceByKey(a => a + 1, 0);
	var b = other.map(e => [e, 0]).reduceByKey(a => a + 1, 0);
	return a.coGroup(b).flatMap(a => (a[1][0].length && a[1][1].length) ? [a[0]] : []);
};

Dataset.prototype.subtract = function (other) {
	var a = this.map(e => [e, 0]).reduceByKey(a => a + 1, 0);
	var b = other.map(e => [e, 0]).reduceByKey(a => a + 1, 0);
	return a.coGroup(b).flatMap(function(a) {
		var res = [];
		if (a[1][0].length && (a[1][1].length == 0))
			for (var i = 0; i < a[1][0][0]; i++) res.push(a[0]);
		return res;
	});
};

Dataset.prototype.keys = function () {return this.map(a => a[0]);};

Dataset.prototype.values = function () {return this.map(a => a[1]);};

Dataset.prototype.lookup = function (key) {
	return this.filter((kv, args) => (kv[0] === args.key), {key: key}).map(kv => kv[1]).collect();
};

Dataset.prototype.countByValue = function () {
	return this.map(e => [e, 1]).reduceByKey((a, b) => a + b, 0).collect();
};

Dataset.prototype.countByKey = function () {
	return this.mapValues(() => 1).reduceByKey((a, b) => a + b, 0).collect();
};

Dataset.prototype.collect = function (opt) {
	opt = opt || {};
	opt.stream = true;
	var reducer = function(a, b) {a.push(b); return a;};
	var combiner = function(a, b) {return a.concat(b);};
	var init = [], action = {args: [], src: reducer, init: init}, self = this;

	return this.sc.runJob(opt, this, action, function(job, tasks) {
		var mainResult = JSON.parse(JSON.stringify(init)), cnt = 0;
		function taskDone(err, res) {
			mainResult = combiner(mainResult, res.data);
			if (++cnt < tasks.length) self.sc.runTask(tasks[cnt], taskDone);
			else {
				for (var i = 0; i < mainResult.length; i++)
					job.stream.write(mainResult[i]);		// FIXME: flow control to be added here
				job.stream.end();
			}
		}

		self.sc.runTask(tasks[cnt], taskDone);
	});
};

Dataset.prototype.first = function(opt) {return this.take(1, opt);};

Dataset.prototype.take = function (N, opt) {
	opt = opt || {};
	opt.stream = true;
	var reducer = function(a, b) {a.push(b); return a;};
	var combiner = function(a, b) {return a.concat(b);};
	var init = [], action = {args: [], src: reducer, init: init}, self = this;

	return this.sc.runJob(opt, this, action, function(job, tasks) {
		var mainResult = JSON.parse(JSON.stringify(init)), cnt = 0;

		function taskDone(err, res) {
			mainResult = combiner(mainResult, res.data);
			if ((++cnt < tasks.length) && (mainResult.length < N)) self.sc.runTask(tasks[cnt], taskDone);
			else {
				for (var i = 0; i < Math.min(N, mainResult.length); i++) job.stream.write(mainResult[i]);
				job.stream.end();
			}
		}

		self.sc.runTask(tasks[cnt], taskDone);
	});
};

Dataset.prototype.top = function (N, opt) {
	opt = opt || {};
	opt.stream = true;
	var reducer = function(a, b) {a.push(b); return a;};
	var combiner = function(a, b) {return b.concat(a);};
	var init = [], action = {args: [], src: reducer, init: init}, self = this;

	return this.sc.runJob(opt, this, action, function(job, tasks) {
		var mainResult = JSON.parse(JSON.stringify(init)), cnt = tasks.length - 1;

		function taskDone(err, res) {
			mainResult = combiner(mainResult, res.data);
			if (--cnt >= 0 && (mainResult.length < N)) self.sc.runTask(tasks[cnt], taskDone);
			else {
				for (var i = mainResult.length - N; i < mainResult.length; i++) job.stream.write(mainResult[i]);
				job.stream.end();
			}
		}

		self.sc.runTask(tasks[cnt], taskDone);
	});
};

Dataset.prototype.aggregate = function (reducer, combiner, init, opt) {
	opt = opt || {};
	var action = {args: [], src: reducer, init: init}, self = this;

	return this.sc.runJob(opt, this, action, function(job, tasks) {
		var mainResult = JSON.parse(JSON.stringify(init)), cnt = 0;

		for (var i = 0; i < tasks.length; i++) {
			self.sc.runTask(tasks[i], function (err, res) {
				mainResult = combiner(mainResult, res.data);
				if (++cnt < tasks.length) return;
				job.stream.write(mainResult);
				job.stream.end();
			});
		}
	});
};

Dataset.prototype.reduce = function (reducer, init, opt) {
	return this.aggregate(reducer, reducer, init, opt);
};

Dataset.prototype.count = function (opt) {
	return this.aggregate(a => a + 1, (a, b) => a + b, 0, opt);
};

Dataset.prototype.forEach = function (eacher, opt) {
	return this.aggregate(eacher, () => null, null, opt);
};

Dataset.prototype.getPartitions = function(done) {
	if (this.partitions == undefined) {
		this.partitions = [];
		var cnt = 0;
		for (var i = 0; i < this.dependencies.length; i++)
			for (var j = 0; j < this.dependencies[i].partitions.length; j++) {
				this.partitions[cnt] = new Partition(this.id, cnt, this.dependencies[i].id, this.dependencies[i].partitions[j].partitionIndex);
				cnt++;
			}
	}
	done();
};

Dataset.prototype.getPreferedLocation = function() {return [];};

function Partition(datasetId, partitionIndex, parentDatasetId, parentPartitionIndex) {
	this.data = [];
	this.datasetId = datasetId;
	this.partitionIndex = partitionIndex;
	this.parentDatasetId = parentDatasetId;
	this.parentPartitionIndex = parentPartitionIndex;
	this.count = 0;
	this.bsize = 0;		// estimated size of memory increment per period
	this.tsize = 0;		// estimated total partition size
	this.skip = false;	// true when partition is evicted due to memory shortage

	this.transform = function(context, data) {
		if (this.skip) return data;	// Passthrough if partition is evicted

		// Periodically check/update available memory, and evict partition
		// if necessary. In this case it will be recomputed if required by
		// a future action.
		if (this.count++ == 9999) {
			this.count = 0;
			if (this.bsize == 0) this.bsize = this.mm.sizeOf(this.data);
			this.tsize += this.bsize;
			this.mm.storageMemory += this.bsize;
			if (this.mm.storageFull()) {
				console.log('# Out of memory, evict partition', this.partitionIndex);
				this.skip = true;
				this.mm.storageMemory -= this.tsize;
				this.data = [];
				this.mm.unregister(this);
				return data;
			}
		}

		// Perform persistence of partition in memory here
		for (var i = 0; i < data.length; i++) this.data.push(data[i]);
		return data;
	};

	this.iterate = function(task, p, pipeline, done) {
		var buffer;
		for (var i = 0; i < this.data.length; i++) {
			buffer = [this.data[i]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	};
}

function Source(sc, N, getItem, args, nPartitions) {
	Dataset.call(this, sc);
	this.getItem = getItem;
	this.nPartitions = nPartitions;
	this.N = N;
	this.args = args;

	this.iterate = function (task, p, pipeline, done) {
		var buffer, i, index = this.bases[p], n = this.sizes[p];
		for (i = 0; i < n; i++, index++) {
			buffer = [this.getItem(index, this.args, task)];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	};
}
util.inherits(Source, Dataset);

Source.prototype.getPartitions = function (done) {
	var P = this.nPartitions || this.sc.worker.length;
	var N = this.N;
	var plen = Math.ceil(N / P);
	var i, max;
	this.partitions = [];
	this.sizes = [];
	this.bases = [];
	for (i = 0, max = plen; i < P; i++, max += plen) {
		this.partitions[i] = new Partition(this.id, i);
		this.sizes[i] = (max <= N) ? plen : (max - N < plen) ? N - (plen * i) : 0;
		this.bases[i] = i ? this.bases[i - 1] + this.sizes[i - 1] : 0;
	}
	done();
};

function parallelize(sc, localArray, P) {
	if (!(localArray instanceof Array))
		throw new Error('First argument of parallelize() is not an instance of Array.');

	return new Source(sc, localArray.length, (i, a) => a[i], localArray, P);
}

function range(sc, start, end, step, P) {
	if (end === undefined) { end = start; start = 0; }
	if (step === undefined) step = 1;

	return new Source(sc, Math.ceil((end - start) / step), (i, a) => i * a.step + a.start, {step, start}, P);
}

function Obj2line() {
	stream.Transform.call(this, {objectMode: true});
}
util.inherits(Obj2line, stream.Transform);

Obj2line.prototype._transform = function (chunk, encoding, done) {
	done(null, JSON.stringify(chunk) + '\n');
};

function Stream(sc, stream, type) { // type = 'line' ou 'object'
	var id = uuid.v4();
	var tmpFile = '/tmp/skale/' + sc.contextId + '/tmp/' + id;
	var targetFile = '/tmp/skale/' + sc.contextId + '/stream/' + id;
	var out = fs.createWriteStream(tmpFile);
	var dataset = sc.textFile(targetFile);

	dataset.watched = true;					// notify skale to wait for file before launching
	dataset.parse = type == 'object';
	out.on('close', function() {
		fs.renameSync(tmpFile, targetFile);
		dataset.watched = false;
	});
	if (type == 'object')
		stream.pipe(new Obj2line()).pipe(out);
	else
		stream.pipe(out);
	return dataset;
}

function TextFile(sc, file, nPartitions) {
	Dataset.call(this, sc);
	this.file = file;

	this.getPartitions = function(done) {
		var self = this;

		function getSplits() {
			var nSplit = nPartitions || sc.worker.length, u = url.parse(self.file);

			if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) splitHDFSFile(u.path, nSplit, mapLogicalSplit);
			else splitLocalFile(u.path, nSplit, mapLogicalSplit);

			function mapLogicalSplit(split) {
				self.splits = split;
				self.partitions = [];
				for (var i = 0; i < self.splits.length; i++)
					self.partitions[i] = new Partition(self.id, i);
				done();
			}
		}

		if (this.watched) {
			var watcher = fs.watch('/tmp/skale/' + sc.contextId + '/stream', function (event, filename) {
				if ((event == 'rename') && (filename == path.basename(self.file))) {
					watcher.close();	// stop watching directory
					getSplits();
				}
			});
		} else getSplits();
	};

	this.iterate = function(task, p, pipeline, done) {
		var buffer;

		function processLine(line) {
			if (!line) return;	// skip empty lines
			buffer = [line];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}

		function processLineParse(line) {
			if (!line) return;	// skip empty lines
			buffer = [JSON.parse(line)];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}

		task.lib.readSplit(this.splits, this.splits[p].index, this.parse ? processLineParse : processLine, done, function (part, opt) {
			return task.getReadStream(part, opt);
		});
	};

	this.getPreferedLocation = function(pid) {return this.splits[pid].ip;};
}

util.inherits(TextFile, Dataset);

function Map(parent, mapper, args) {
	Dataset.call(this, parent.sc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function map(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp[i] = this.mapper(data[i], this.args, this.global);
		return tmp;
	};
}

util.inherits(Map, Dataset);

function FlatMap(parent, mapper, args) {
	Dataset.call(this, parent.sc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function flatmap(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp = tmp.concat(this.mapper(data[i], this.args, this.global));
		return tmp;
	};
}

util.inherits(FlatMap, Dataset);

function MapValues(parent, mapper, args) {
	Dataset.call(this, parent.sc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp[i] = [data[i][0], this.mapper(data[i][1], this.args, this.global)];
		return tmp;
	};
}

util.inherits(MapValues, Dataset);

function FlatMapValues(parent, mapper, args) {
	Dataset.call(this, parent.sc, [parent]);
	this.mapper = mapper;
	this.args = args;

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) {
			var t0 = this.mapper(data[i][1], this.args, this.global);
			tmp = tmp.concat(t0.map(function(e) {return [data[i][0], e];}));
		}
		return tmp;
	};
}

util.inherits(FlatMapValues, Dataset);

function Filter(parent, filter, args) {
	Dataset.call(this, parent.sc, [parent]);
	this.filter = filter;
	this.args = args;

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			if (this.filter(data[i], this.args, this.global)) tmp.push(data[i]);
		return tmp;
	};
}

util.inherits(Filter, Dataset);

function Random(seed) {
	seed = seed || 0;
	this.x = 123456789 + seed;
	this.y = 188675123;

	// xorshift RNG producing a sequence of 2 ** 64 - 1 32 bits integers
	// See http://www.jstatsoft.org/v08/i14/paper by G. Marsaglia
	this.next = function() {
		var t = this.x, u = this.y;
		t ^= t << 8;
		this.x = u;
		return this.y = (u ^ (u >> 22)) ^ (t ^ (t >> 9));
	};

	// Return a float in range [0, 1) like Math.Random()
	this.nextDouble = function () {
		return this.next() / 4294967296.0;
	};
}

function Poisson(lambda, initSeed) {
	initSeed = initSeed || 1;

	const rng = new Random(initSeed);

	this.sample = function () {
		var L = Math.exp(-lambda), k = 0, p = 1;
		do {
			k++;
			p *= rng.nextDouble();
		} while (p > L);
		return k - 1;
	};
}

function Sample(parent, withReplacement, frac, seed) {
	Dataset.call(this, parent.sc, [parent]);
	this.withReplacement = withReplacement;
	this.frac = frac;
	this.rng = withReplacement ? new Poisson(frac, seed) : new Random(seed);

	this.transform = function(context, data) {
		var tmp = [], i, j;
		if (this.withReplacement) {
			for (i = 0; i < data.length; i++)
				for (j = 0; j < this.rng.sample(); j++) tmp.push(data[i]);
		} else {
			for (i = 0; i < data.length; i++)
				if (this.rng.nextDouble() < this.frac) tmp[i] = data[i];
		}
		return tmp;
	};
}

util.inherits(Sample, Dataset);

function Union(sc, parents) {
	Dataset.call(this, sc, parents);

	this.transform = function(context, data) {return data;};
}

util.inherits(Union, Dataset);

function AggregateByKey(sc, dependencies, reducer, combiner, init, args) {
	Dataset.call(this, sc, dependencies);
	this.combiner = combiner;
	this.reducer = reducer;
	this.init = init;
	this.args = args;
	this.shuffling = true;
	this.executed = false;
	this.buffer = [];

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = 0, i;
			this.partitions = [];
			for (i = 0; i < this.dependencies.length; i++)
				P = Math.max(P, this.dependencies[i].partitions.length);
			for (i = 0; i < P; i++) this.partitions[i] = new Partition(this.id, i);
			this.partitioner = new HashPartitioner(P);
		}
		done();
	};

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0], value = data[i][1], str = JSON.stringify(key), pid = this.partitioner.getPartitionIndex(data[i][0]);
			if (this.buffer[pid] == undefined) this.buffer[pid] = {};
			if (this.buffer[pid][str] == undefined) this.buffer[pid][str] = JSON.parse(JSON.stringify(this.init));
			this.buffer[pid][str] = this.reducer(this.buffer[pid][str], value, this.args, this.global);
		}
	};

	this.spillToDisk = function(task, done) {
		var i, isLeft, str, hash, data, path;

		if (this.dependencies.length > 1) {									// COGROUP
			isLeft = (this.shufflePartitions[task.pid].parentDatasetId == this.dependencies[0].id);
			for (i = 0; i < this.partitions.length; i++) {
				str = '';
				path = task.basedir + 'shuffle/' + task.lib.uuid.v4();
				for (hash in this.buffer[i]) {
					data = isLeft ? [JSON.parse(hash), [this.buffer[i][hash], []]] : [JSON.parse(hash), [[], this.buffer[i][hash]]];
					str += JSON.stringify(data) + '\n';
				}
				task.lib.fs.appendFileSync(path, str);
				task.files[i] = {host: task.grid.host.uuid, path: path};
			}
		} else {															// AGGREGATE BY KEY
			for (i = 0; i < this.partitions.length; i++) {
				str = '';
				path = task.basedir + 'shuffle/' + task.lib.uuid.v4();
				for (hash in this.buffer[i]) {
					data = [JSON.parse(hash), this.buffer[i][hash]];
					str += JSON.stringify(data) + '\n';
				}
				task.lib.fs.appendFileSync(path, str);
				task.files[i] = {host: task.grid.host.uuid, path: path};
			}
		}
		done();
	};

	this.iterate = function(task, p, pipeline, done) {
		var self = this, cbuffer = {}, cnt = 0, files = [];

		for (var i = 0; i < self.shufflePartitions.length; i++)
			files.push(self.shufflePartitions[i].files[p]);

		processShuffleFile(files[cnt], processDone);

		function processShuffleFile(file, done) {
			var lines = new task.lib.Lines();
			task.getReadStream(file).pipe(lines);
			lines.on('data', function(linev) {
				for (var i = 0; i < linev.length; i++) {
					var data = JSON.parse(linev[i]), key = data[0], value = data[1], hash = JSON.stringify(key);
					if (cbuffer[hash] != undefined) cbuffer[hash] = self.combiner(cbuffer[hash], value, self.args, self.global);
					else cbuffer[hash] = value;
				}
			});
			lines.on('end', done);
		}

		function processDone() {
			if (++cnt == files.length) {
				for (var key in cbuffer) {
					var buffer = [[JSON.parse(key), cbuffer[key]]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
				done();
			} else processShuffleFile(files[cnt], processDone);
		}
	};
}

util.inherits(AggregateByKey, Dataset);

function Cartesian(sc, dependencies) {
	Dataset.call(this, sc, dependencies);
	this.shuffling = true;
	this.executed = false;
	this.buffer = [];

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = this.dependencies[0].partitions.length * this.dependencies[1].partitions.length;
			this.partitions = [];
			for (var i = 0; i < P; i++)
				this.partitions[i] = new Partition(this.id, i);
		}
		done();
	};

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) this.buffer.push(data[i]);
	};

	this.spillToDisk = function(task, done) {
		var str = '', path = task.basedir + 'shuffle/' + task.lib.uuid.v4();
		for (var i = 0; i < this.buffer.length; i++)
			str += JSON.stringify(this.buffer[i]) + '\n';
		task.lib.fs.appendFileSync(path, str);
		task.files = {host: task.grid.host.uuid, path: path};
		done();
	};

	this.iterate = function(task, p, pipeline, done) {
		var pleft = this.dependencies[0].partitions.length;
		var pright = this.dependencies[1].partitions.length;
		var p1 = Math.floor(p / pright);
		var p2 = p % pright + pleft;
		var self = this;
		var s1 = '';
		var stream1 = task.getReadStream(this.shufflePartitions[p1].files, {encoding: 'utf8'});
		stream1.on('data', function (s) {s1 += s;});
		stream1.on('end', function () {
			var a1 = s1.split('\n');
			var s2 = '';
			var stream2 = task.getReadStream(self.shufflePartitions[p2].files, {encoding: 'utf8'});
			stream2.on('data', function (s) {s2 += s;});
			stream2.on('end', function () {
				var a2 = s2.split('\n');
				for (var i = 0; i < a1.length; i++) {
					if (a1[i] == '') continue;
					for (var j = 0; j < a2.length; j++) {
						if (a2[j] == '') continue;
						var buffer = [[JSON.parse(a1[i]), JSON.parse(a2[j])]];
						for (var t = 0; t < pipeline.length; t++)
							buffer = pipeline[t].transform(pipeline[t], buffer);
					}
				}
				done();
			});
		});
	};
}

util.inherits(Cartesian, Dataset);

function SortBy(sc, dependencies, keyFunc, ascending, numPartitions) {
	Dataset.call(this, sc, [dependencies]);
	this.shuffling = true;
	this.executed = false;
	this.keyFunc = keyFunc;
	this.ascending = (ascending == undefined) ? true : ascending;
	this.buffer = [];

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = Math.max(numPartitions || 1, this.dependencies[0].partitions.length);

			this.partitions = [];
			for (var p = 0; p < P; p++) this.partitions.push(new Partition(this.id, p));
			this.partitioner = new RangePartitioner(P, keyFunc, this.dependencies[0]);
			this.partitioner.init(done);
		} else done();
	};

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var pid = this.partitioner.getPartitionIndex(this.keyFunc(data[i]));
			if (this.buffer[pid] == undefined) this.buffer[pid] = [];
			this.buffer[pid].push(data[i]);
		}
	};

	this.spillToDisk = function(task, done) {
		for (var i = 0; i < this.partitions.length; i++) {
			var str = '', path = task.basedir + 'shuffle/' + task.lib.uuid.v4();
			if (this.buffer[i] != undefined)
				for (var j = 0; j < this.buffer[i].length; j++)
					str += JSON.stringify(this.buffer[i][j]) + '\n';
			task.lib.fs.appendFileSync(path, str);
			task.files[i] = {host: task.grid.host.uuid, path: path};
		}
		done();
	};

	this.iterate = function(task, p, pipeline, done) {
		var self = this, cbuffer = [], cnt = 0, files = [];

		for (var i = 0; i < self.shufflePartitions.length; i++)
			files.push(self.shufflePartitions[i].files[p]);

		processShuffleFile(files[cnt], processDone);

		function processShuffleFile(file, done) {
			var lines = new task.lib.Lines();
			task.getReadStream(file).pipe(lines);
			lines.on('data', function (linev) {
				for (var i = 0; i < linev.length; i++)
					cbuffer.push(JSON.parse(linev[i]));
			});
			lines.on('end', done);
		}

		function processDone() {
			if (++cnt == files.length) {
				cbuffer.sort(compare);
				for (var i = 0; i < cbuffer.length; i++) {
					var buffer = [cbuffer[i]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
				done();
			} else processShuffleFile(files[cnt], processDone);

			function compare(a, b) {
				if (self.keyFunc(a) < self.keyFunc(b)) return self.ascending ? -1 : 1;
				if (self.keyFunc(a) > self.keyFunc(b)) return self.ascending ? 1 : -1;
				return 0;
			}
		}
	};
}

util.inherits(SortBy, Dataset);

function PartitionBy(sc, dependencies, partitioner) {
	Dataset.call(this, sc, [dependencies]);
	this.shuffling = true;
	this.executed = false;
	this.buffer = [];
	this.partitioner = partitioner;

	this.getPartitions = function(done) {
		if (this.partitions == undefined) {
			var P = this.partitioner.numPartitions;
			this.partitions = [];
			for (var p = 0; p < P; p++) this.partitions.push(new Partition(this.id, p));
			if (this.partitioner.init) this.partitioner.init(done);
			else done();
		} else done();
	};

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var pid = this.partitioner.getPartitionIndex(data[i][0]);
			if (this.buffer[pid] == undefined) this.buffer[pid] = [];
			this.buffer[pid].push(data[i]);
		}
	};

	this.spillToDisk = function(task, done) {
		for (var i = 0; i < this.partitions.length; i++) {
			var str = '', path = task.basedir + 'shuffle/' + task.lib.uuid.v4();
			if (this.buffer[i] != undefined)
				for (var j = 0; j < this.buffer[i].length; j++)
					str += JSON.stringify(this.buffer[i][j]) + '\n';
			task.lib.fs.appendFileSync(path, str);
			task.files[i] = {host: task.grid.host.uuid, path: path};
		}
		done();
	};

	this.iterate = function(task, p, pipeline, done) {
		var self = this, cbuffer = [], cnt = 0, files = [];
		for (var i = 0; i < self.shufflePartitions.length; i++)
			files.push(self.shufflePartitions[i].files[p]);

		processShuffleFile(files[cnt], processDone);

		function processShuffleFile(file, done) {
			var lines = new task.lib.Lines();
			task.getReadStream(file).pipe(lines);
			lines.on('data', function(linev) {
				for (var i = 0; i < linev.length; i++)
					cbuffer.push(JSON.parse(linev[i]));
			});
			lines.on('end', done);
		}

		function processDone() {
			if (++cnt == files.length) {
				for (var i = 0; i < cbuffer.length; i++) {
					var buffer = [cbuffer[i]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
				done();
			} else processShuffleFile(files[cnt], processDone);
		}
	};
}

util.inherits(PartitionBy, Dataset);

function RangePartitioner(numPartitions, keyFunc, dataset) {
	this.numPartitions = numPartitions;

	this.init = function(done) {
		var self = this;
		dataset.sample(false, 0.5).collect().toArray(function(err, result) {
			function compare(a, b) {
				if (keyFunc(a) < keyFunc(b)) return -1;
				if (keyFunc(a) > keyFunc(b)) return 1;
				return 0;
			}
			result.sort(compare);
			self.upperbounds = [];
			if (result.length <= numPartitions - 1) {
				self.upperbounds = result;	// supprimer les doublons peut-etre ici
			} else {
				var s = Math.floor(result.length / numPartitions);
				for (var i = 0; i < numPartitions - 1; i++) self.upperbounds.push(result[s * (i + 1)]);
			}
			done();
		});
	};

	this.getPartitionIndex = function(data) {
		for (var i = 0; i < this.upperbounds.length; i++)
			if (data < this.upperbounds[i]) break;
		return i;
	};
}

function HashPartitioner(numPartitions) {
	this.numPartitions = numPartitions;

	this.hash = function (o) {
		var i, h = 0, s = o.toString(), len = s.length;
		for (i = 0; i < len; i++) {
			h = ((h << 5) - h) + s.charCodeAt(i);
			h = h & h;	// convert to 32 bit integer
		}
		return Math.abs(h);
	};

	this.getPartitionIndex = function(data) {
		return this.hash(data) % this.numPartitions;
	};
}

module.exports = {
	Dataset: Dataset,
	Partition: Partition,
	parallelize: parallelize,
	range: range,
	TextFile: TextFile,
	Source: Source,
	Stream: Stream,
	Random: Random,
	RangePartitioner: RangePartitioner,
	HashPartitioner: HashPartitioner
};
