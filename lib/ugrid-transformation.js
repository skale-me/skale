'use strict';

var fs = require('fs');

var ml = require('./ugrid-ml.js');
var trace = require('line-trace');
var Lines = require('./lines.js'), Lines2 = require('./lines2.js');

module.exports.UgridJob = function(grid, app, param) {
	this.id = param.jobId;
	this.sendResultSemaphore = 0;
	this.rdd = {};

	var stream = grid.createWriteStream(this.id, app.master_uuid), stageBoundaries = [], stages = [], self = this;

	// Step 1: Spawn RDDs from top to bottom
	for (var i = 0; i < param.node.length; i++) {
		if (app.rdd[param.node[i].id] == undefined) {
			switch (param.node[i].type) {
			case 'parallelize': this.rdd[param.node[i].id] = new ParallelizedRDD(grid, app, this, param.node[i]); break;
			case 'randomSVMData': this.rdd[param.node[i].id] = new RandomSVMData(grid, app, this, param.node[i]); break;
			case 'textFile': this.rdd[param.node[i].id] = new TextFileRDD(grid, app, this, param.node[i]); break;
			case 'map': this.rdd[param.node[i].id] = new MappedRDD(grid, app, this, param.node[i]); break;
			case 'union': this.rdd[param.node[i].id] = new UnionedRDD(grid, app, this, param.node[i]); break;
			case 'filter': this.rdd[param.node[i].id] = new FilteredRDD(grid, app, this, param.node[i]); break;
			case 'flatMap': this.rdd[param.node[i].id] = new FlatMappedRDD(grid, app, this, param.node[i]); break;
			case 'flatMapValues': this.rdd[param.node[i].id] = new FlatMappedValuesRDD(grid, app, this, param.node[i]); break;
			case 'mapValues': this.rdd[param.node[i].id] = new mappedValuesRDD(grid, app, this, param.node[i]); break;
			case 'groupByKey': this.rdd[param.node[i].id] = new GroupedByKeyRDD(grid, app, this, param.node[i]); break;
			case 'reduceByKey': this.rdd[param.node[i].id] = new ReducedByKeyRDD(grid, app, this, param.node[i]); break;
			case 'distinct': this.rdd[param.node[i].id] = new DistinctRDD(grid, app, this, param.node[i]); break;
			case 'sample': this.rdd[param.node[i].id] = new SampledRDD(grid, app, this, param.node[i]); break;			
			default: console.error('Not yet implemented');
			}
		} else this.rdd[param.node[i].id] = app.rdd[param.node[i].id];
		if (this.rdd[param.node[i].id].shuffling) stageBoundaries.unshift(this.rdd[param.node[i].id]);
	}

	// Step 2: Find stages
	for (var i = 0; i < stageBoundaries.length; i++) {
		if (stageBoundaries[i].partitions) break;
		stages.unshift(stageBoundaries[i]);
	}

	this.sendResult = function(result) {
		if (result) self.result = result;
		if ((app.wid != 0) && (++self.sendResultSemaphore != 2)) return;
		stream.write(self.result);
		if (app.worker[app.wid + 1])
			grid.send(app.worker[app.wid + 1].uuid, {cmd: 'action', jobId: self.id});
		grid.send(app.master_uuid, {cmd: 'endJob', data: self.id});
	}

	// Method for RDDs subject to shuffle
	this.run = function() {
		var root = this.rdd[param.node[param.node.length - 1].id], s = 0;
		(function nextStage() {
			if (s < stages.length) stages[s++].run(nextStage);
			else root[param.action.fun](self.sendResult, param.action);
		})();
	}
};

function recompile(s) {
	var args = s.match(/\(([^)]*)/)[1];
	var body = s.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
	return new Function(args, body);
}

function Partition(source, data, key) {
	this.source = source;		// {id: 1234, p: 0}
	this.data = data;
	this.key = key;
	var self = this;	// when class method is called with a different 'this'

	this.save = function(array) {
		for (var i = 0; i < array.length; i++)
			self.data.push(array[i]);
		return array;
	}

	// Unused flow controlled write to disk
	this.saveToDisk = function(array, done) {
		var i = 0, drain, stream; // stream must be declared
		(function write() {
			do {drain = stream.write(JSON.stringify(array[i]))} while((++i < array.length) && drain);
			if (i == array.length) done(array);
			else stream.once('drain', write);
		})();
	}

	this.iterate = function(pipeline, done) {
		var buffer;
		for (var i = 0; i < self.data.length; i++) {
			buffer = [self.data[i]];
			for (var t = 0; t < pipeline.length; t++) 
				buffer = pipeline[t](buffer);
		}
		done();
	}
}

function ParallelizedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	this.partitions = [];
	var partitions = param.args || [];
	for (var p = 0; p < partitions.length; p++)
		this.partitions.push(new Partition(null, partitions[p]));
}

function RandomSVMData(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var D = param.args[0];
	var partitions = param.args[1] || [];

	this.partitions = [];
	for (var p = 0; p < partitions.length; p++)
		this.partitions.push(new Partition(null));

	this.iterate = function(p, pipeline, done) {
		var rng = new ml.Random(partitions[p].seed), buffer;

		for (var i = 0; i < partitions[p].n; i++) {
			buffer = [ml.randomSVMLine(rng, D)];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t](buffer);
		}
		done();
	}
}

function TextFileRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var self = this, blocks = param.args[2], hashedBlocks = {};

	this.partitions = [];
	for (var p = 0; p < blocks.length; p++) {
		this.partitions.push(new Partition(null));
		blocks[p].p = p;
		hashedBlocks[blocks[p].bid] = blocks[p];
	}

	this.iterate = function(p, pipeline, done) {
		self.pipeline = pipeline;
		self.done = done;

		var lines = new Lines(), buffer;
		fs.createReadStream(blocks[p].file, blocks[p].opt).pipe(lines);

		lines.once("data", function (line) {
			blocks[p].firstLine = line;
			lines.once("data", function (line) {
				blocks[p].lastLine = line;
				lines.on("data", function (line) {
					buffer = [blocks[p].lastLine];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t](buffer);
					blocks[p].lastLine = line;					
				});
			});
		});

		function shuffleLine(shuffledLine) {
			var shuffleTo = blocks[p].shuffleTo;
			if (shuffleTo != app.wid) {
				grid.send(app.worker[shuffleTo].uuid, {
					cmd: 'lastLine', 
					args: {lastLine: shuffledLine, rddId: self.id, bid: blocks[p].bid + 1}, 
					jobId: job.id
				});
			} else processLastLine({lastLine: shuffledLine, bid: blocks[p].bid + 1});
		}

		lines.on("endNewline", function(lastLineComplete) {
			var firstLineProcessed;
			var isFirstBlock = (blocks[p].skipFirstLine === false);
			var isLastBlock = (blocks[p].shuffleLastLine === false);
			var hasLastLine = blocks[p].lastLine !== undefined;

			blocks[p].hasScannedFile = true;
			// FIRST LINE
			if (isFirstBlock) {
				firstLineProcessed = true;
				if (hasLastLine || lastLineComplete || isLastBlock)
					processLine(blocks[p].p, blocks[p].firstLine, true, pipeline);
				if (!hasLastLine && !lastLineComplete && !isLastBlock)
					shuffleLine(blocks[p].firstLine);
			} else {
				blocks[p].forward = (!hasLastLine && !lastLineComplete) ? true : false;
				firstLineProcessed = processFirstLine(blocks[p].bid);
			}
			// LAST LINE
			if (hasLastLine) {
				if (isLastBlock) processLine(blocks[p].p, blocks[p].lastLine, undefined, pipeline);
				else if (lastLineComplete) {
					processLine(blocks[p].p, blocks[p].lastLine, undefined, pipeline);
					shuffleLine('');
				} else shuffleLine(blocks[p].lastLine);
			} else if (lastLineComplete && !isLastBlock) shuffleLine('');

			if (firstLineProcessed) done();
		});
	}

	function processLine(p, line, first, pipeline) {
		var buffer = [line];
		for (var t = 0; t < pipeline.length; t++)
			buffer = pipeline[t](buffer);
	}

	var processLastLine = this.processLastLine = function(data) {
		var targetBlock = hashedBlocks[data.bid];
		targetBlock.rxLastLine = data.lastLine;
		targetBlock.hasReiceivedLastLine = true;
		var firstLineProcessed = processFirstLine(data.bid);

		if (firstLineProcessed) self.done();
	};

	function processFirstLine(bid) {
		var targetBlock = hashedBlocks[bid];
		if (!targetBlock.hasReiceivedLastLine || !targetBlock.hasScannedFile) return false;
		if (targetBlock.forward && targetBlock.shuffleLastLine) {
			var shuffledLine = targetBlock.rxLastLine + targetBlock.firstLine;
			if (targetBlock.shuffleTo != app.wid) {
				grid.send(app.worker[targetBlock.shuffleTo].uuid, {
					cmd: 'lastLine', 
					args: {lastLine: shuffledLine, rddId: self.id, bid: bid + 1}
				});
			} else processLastLine({lastLine: shuffledLine, bid: bid + 1});
		} else {
			var str = (targetBlock.rxLastLine === undefined) ? targetBlock.firstLine : targetBlock.rxLastLine + targetBlock.firstLine;
			processLine(targetBlock.p, str, true, self.pipeline);
		}
		return true;
	}
}

function RDD(grid, app, job, param) {
	this.id = param.id;
	this.dependencies = param.dependencies;
	this.persistent = param.persistent;
	this.partitions;
	this.nShuffle = 0;
	this.nextStage;
	this.type = param.type;

	var self = this;

	function findPartitions(n) {
		if (n.partitions == undefined) {
			n.partitions = [];
			for (var i = 0; i < n.dependencies.length; i++) {
				var parentPartitions = findPartitions(job.rdd[n.dependencies[i]]);
				for (var p = 0; p < parentPartitions.length; p++)
					n.partitions.push(new Partition({id: n.dependencies[i], p: p}));
			}
		}
		return n.partitions;
	}

	// Attention à la gestion de la mise en tete de la data dans le cas du textFile
	this.collect = function(callback) {
		var result = {}, p = -1;

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) {
				var dest = result[p] = [];
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					for (var i = 0; i < data.length; i++) dest.push(data[i]);
				});
			} else callback(result);
		})();
	}

	this.count = function(callback) {
		var result = 0, p = -1;

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) 
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					result += data.length;
				});
			else callback(result);
		})();
	};

	this.reduce = function(callback, param) {
		var result = JSON.parse(JSON.stringify(param.args[0])), p = -1;
		var reducer = recompile(param.src);

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) {
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					for (var i = 0; i < data.length; i++) 
						result = reducer(result, data[i]);
				});
			} else callback(result);
		})();
	};

	this.countByValue = function(callback) {
		var result = {}, p = -1;

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) {
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					for (var i = 0; i < data.length; i++) {
						var str = JSON.stringify(data[i]);
						if (result[str] === undefined) result[str] = [data[i], 0];
						result[str][1]++;
					}
				});
			} else callback(result);
		})();
	};

	this.lookup = function(callback, param) {
		var result = {}, key = param.args[0], p = -1;

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) {
				var dest = result[p] = [];
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					for (var i = 0; i < data.length; i++)
						if (data[i][0] == key) dest.push(data[i]);
				});
			} else callback(result);
		})();
	};

	this.take = function(callback, param) {
		var result = [], num = param.args[0], p = -1;

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) {
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					if (result.length < num)
						result = result.concat(data).slice(0, num);
				});
			} else callback(result);
		})();
	};

	this.takeOrdered = function(callback, param) {
		var result = [], num = param.args[0], p = -1;
		var sorter = recompile(param.src);

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) {
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					result = result.concat(data).sort(sorter).slice(0, num);
				});
			} else callback(result);
		})();
	};

	this.top = function(callback, param) {
		var result = [], num = param.args[0], p = -1;
		var sorter;
		// var sorter = recompile(param.src);	// BUG ICI le sorter n'existe pas dans les paramètres

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) {
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {
					result = result.concat(data).sort(sorter).slice(0, num);
				});
			} else callback(result);
		})();
	};

	this.forEach = function(callback, param) {
		var each = recompile(param.src), p = -1;

		findPartitions(this);
		(function action() {
			if (++p < self.partitions.length) 
				self.pipeline(p, function() {process.nextTick(action)}, function (data) {data.forEach(each);});
			else callback(result);
		})();
	};

	this.pipeline = function(p, done, action) {
		var sourceRDD = self, sourcePartitionIdx = p, sourcePartition = self.partitions[p];
		var pipeline = action ? [action] : [];

		// Build p-th partition pipeline
		while ((sourcePartition.data == undefined) && sourcePartition.source) {
			if (sourceRDD.persistent && !sourceRDD.shuffling) {			// TO DEBUG partial storage RAM, add && (p == 0)
				sourcePartition.data = [];
				pipeline.unshift(sourcePartition.save);
			}
			if (sourceRDD.transform) pipeline.unshift(sourceRDD.transform);
			sourceRDD = job.rdd[sourcePartition.source.id];
			sourcePartitionIdx = sourcePartition.source.p;
			sourcePartition = sourceRDD.partitions[sourcePartitionIdx];
		}

		if (sourcePartition.data) sourcePartition.iterate(pipeline, done);
		else {
			// cas pas de parents
			if (sourceRDD.persistent) {
				sourcePartition.data = [];
				pipeline.unshift(sourcePartition.save);
			}
			sourceRDD.iterate(sourcePartitionIdx, pipeline, done);
		}
	}

	if (this.persistent) app.rdd[param.id] = this;

	// for shuffle RDDs
	this.run = function(nextStage) {
		this.nextStage = nextStage;		// nextStage() will be called elsewhere
		var p = -1;

		findPartitions(this);
		(function run() {
			if (++p < self.partitions.length) self.pipeline(p, function() {process.nextTick(run)});
			else {
				self.preShuffle();
				for (var i = 0; i < app.worker.length; i++)
					if (grid.host.uuid == app.worker[i].uuid) self.shuffle();
					else grid.send(app.worker[i].uuid, {cmd: 'shuffle', jobId: job.id, rddId: self.id});
			}
		})();
	}

	this.base_dir = '/tmp/' + this.id + '/';
	this.shuffle_dir = this.base_dir;							// shuffle_dir is base_dir for now
	var worker_dir = this.base_dir + 'worker_' + app.wid + '/';
	this.intermediate_dir = worker_dir + 'intermediate/';	
	var partition_dir = worker_dir + 'partitions/';
	var P = 1;		// number of default partitions

	this.wFile = {};
	this.pFile = {};
	this.tmp_dir = worker_dir + 'tmp/';
	this.MAX_BUFFER_SIZE = 8192;

	// Create RDD directory files
	try {fs.mkdirSync(this.base_dir)} catch (e) {};
	try {fs.mkdirSync(worker_dir)} catch (e) {};
	try {fs.mkdirSync(this.tmp_dir)} catch (e) {};
	try {fs.mkdirSync(this.intermediate_dir)} catch (e) {};
	try {fs.mkdirSync(partition_dir)} catch (e) {};
	for (var to = 0; to < app.worker.length; to++) {
		this.wFile[to] = {
			name: self.base_dir + 'w_' + app.wid + '_' + to,
			buffer: ''
		}
		try {fs.unlinkSync(this.wFile[to].name);} catch (e){;}			// DEBUG
		fs.appendFileSync(this.wFile[to].name, '');
	}

	this.shuffle = function() {
		if (++this.nShuffle < app.worker.length) return;
		// create partitions and partition files
		this.partitions = [];
		for (var p = 0; p < P; p++) {
			this.partitions.push(new Partition(null));
			this.pFile[p] = {
				name: partition_dir + 'p_' + app.wid + '_' + p,
				buffer: ''
			}
			try {fs.unlinkSync(this.pFile[p].name);} catch (e){;}		// DEBUG
			fs.appendFileSync(this.pFile[p].name, '');
		}
		this.postShuffle();
	}
}

// ---------------------------------------------------------------------------------------------------- //
// Narrow Transformations
// ---------------------------------------------------------------------------------------------------- //
function MappedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var mapper = recompile(param.src);

	this.transform = function(data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp[i] = mapper(data[i], param.args[0]);
		return tmp;
	}
}

function UnionedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
}

function FilteredRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var filter = recompile(param.src);

	this.transform = function(data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			if (filter(data[i])) tmp.push(data[i]);
		return tmp;
	}
}

function FlatMappedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var mapper = recompile(param.src);

	this.transform = function(data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) tmp = tmp.concat(mapper(data[i]));
		return tmp;
	}
}

function FlatMappedValuesRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var mapper = recompile(param.src);

	this.transform = function(data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) {
			var t0 = mapper(data[i][1]);
			tmp = tmp.concat(t0.map(function(e) {return [data[i][0], e];}));
		}
		return tmp;
	}
}

function mappedValuesRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	var mapper = recompile(param.src);

	this.transform = function(data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) tmp[i] = [data[i][0], mapper(data[i][1])];
		return tmp;
	}
}

// ---------------------------------------------------------------------------------------------------- //
// Shuffle Transformations
// ---------------------------------------------------------------------------------------------------- //
function DistinctRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var self = this;

	this.shuffling = true;

	this.transform = function(data) {
		for (var i = 0; i < data.length; i++) {
			var str = JSON.stringify(data[i]);
			var wid = ml.cksum(str) % app.worker.length;
			self.wFile[wid].buffer += str + '\n';
			if (self.wFile[wid].buffer.length > self.MAX_BUFFER_SIZE) {
				fs.appendFileSync(self.wFile[wid].name, self.wFile[wid].buffer);
				self.wFile[wid].buffer = '';
			}
		}
	};

	this.preShuffle = function() {
		for (var i = 0; i < app.worker.length; i++)
			if (this.wFile[i].buffer.length) {
				fs.appendFileSync(this.wFile[i].name, this.wFile[i].buffer);
				this.wFile[i].buffer = '';
			}
	}

	this.postShuffle = function() {
		// Population des fichiers de partitions à partir des fichiers de shuffles
		function populate(from, to, done) {
			var lines = new Lines();
			fs.createReadStream(self.shuffle_dir + 'w_' + from + '_' + to).pipe(lines);
			lines.on('data', function(line) {
				var fid = ml.cksum(line) % self.partitions.length;
				self.pFile[fid].buffer += line + '\n';
				if (self.pFile[fid].buffer.length > self.MAX_BUFFER_SIZE) {
					fs.appendFileSync(self.pFile[fid].name, self.pFile[fid].buffer);
					self.pFile[fid].buffer = '';
				}
			});
			lines.on('end', function() {
				if (++from < app.worker.length) populate(from, to, done);
				else {
					for (var fid = 0; fid < self.partitions.length; fid++)
						if (self.pFile[fid].buffer.length)
							fs.appendFileSync(self.pFile[fid].name, self.pFile[fid].buffer);
					done();
				}
			});
		}
		populate(0, app.wid, this.nextStage);
	}

	this.iterate = function(p, pipeline, done) {
		var lines = new Lines(), index = [];
		try {
			fs.createReadStream(self.pFile[p].name).pipe(lines);
			lines.on('data', function(line) {
				if (index.indexOf(line) != -1) return;
				index.push(line);
				var buffer = [JSON.parse(line)];
				for (var t = 0; t < pipeline.length; t++)
					buffer = pipeline[t](buffer);
			});
			lines.on('end', done);
		} catch (e) {done();}
	}
};

function GroupedByKeyRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var self = this;

	this.shuffling = true;

	// Ici on peut bufferiser en stockant  plusieurs clé dans un objet,
	// pour chaque clé on regarde si le quota est atteint, on écrit dans fichier et on delete la clé,
	// on regarde ensuite la taille totale des buffers des clés, si c'est supérieur
	// au quota total, alors on écrit toutes les clés et on les delete
	this.transform = function(data) {
		for (var i = 0; i < data.length; i++) {
			var key = JSON.stringify(data[i][0]);
			var value = JSON.stringify(data[i][1]);
			fs.appendFileSync(self.tmp_dir + key, value + '\n');
		}
	};

	this.preShuffle = function() {
		var keys = fs.readdirSync(this.tmp_dir);
		for (var i = 0; i < keys.length; i++) {
			var wid = ml.cksum(keys[i]) % app.worker.length;
			var content = fs.readFileSync(this.tmp_dir + keys[i], {encoding: 'utf8'});
			this.wFile[wid].buffer += '[' + keys[i] + ', [' + content.replace(/\n/, ',').slice(0, -1) + ']]\n';
			if (this.wFile[wid].buffer.length > this.MAX_BUFFER_SIZE) {
				fs.appendFileSync(this.wFile[wid].name, this.wFile[wid].buffer);
				this.wFile[wid].buffer = '';
			}
		}
		for (var i = 0; i < app.worker.length; i++)
			if (this.wFile[i].buffer.length)
				fs.appendFileSync(this.wFile[i].name, this.wFile[i].buffer);
	}

	this.postShuffle = function() {
		function populate(from, to, done) {
			var lines = new Lines();
			fs.createReadStream(self.shuffle_dir + 'w_' + from + '_' + to).pipe(lines);
			lines.on('data', function(line) {
				var data = JSON.parse(line);
				var key = JSON.stringify(data[0]), value = JSON.stringify(data[1]);
				fs.appendFileSync(self.intermediate_dir + key, value + '\n');
			});
			lines.on('end', function() {
				if (++from < app.worker.length) populate(from, to, done);
				else done();
			});
		}
		populate(0, app.wid, this.nextStage);
	}

	this.iterate = function(p, pipeline, done) {
		var keys = fs.readdirSync(self.intermediate_dir), i = 0;
		var pKeys =	keys.filter(function(key) {
			return ((ml.cksum(key) % self.partitions.length) == p)
		});

		function populate(key_str, done) {
			var lines = new Lines();
			var key = JSON.parse(key_str);
			fs.createReadStream(self.intermediate_dir + key_str).pipe(lines);
			var buffer = [key, []];
			lines.on('data', function(line) {
				var data = JSON.parse(line);
				for (var v = 0; v < data.length; v++) buffer[1].push(data[v]);
			});
			lines.on('end', function() {
				for (var t = 0; t < pipeline.length; t++)
					buffer = pipeline[t](buffer);
				if (++i < pKeys.length) populate(pKeys[i], done);
				else done();
			});
		}
		if (pKeys.length) populate(pKeys[0], done);
		else done();
	}
}

// function ReducedByKeyRDD(grid, app, job, param) {
// 	RDD.call(this, grid, app, job, param);

// 	this.shuffling = true;
// 	var self = this;
// 	var map = this.map = app.worker.map(function() {return {};});
// 	var SRAM = [];

// 	var reducer = recompile(param.src);
// 	var initVal = param.args[0];

// 	this.transform = function(data) {
// 		for (var i = 0; i < data.length; i++) {
// 			var key = data[i][0];
// 			var wid = ml.cksum(key) % app.worker.length;
// 			if (map[wid][key] === undefined)
// 				map[wid][key] = [[key, JSON.parse(JSON.stringify(initVal))]];
// 			map[wid][key][0][1] = reducer(map[wid][key][0][1], data[i][1]);
// 		}
// 	};

// 	this.shuffle = function(data) {
// 		for (var key in data) {
// 			for (var i = 0; i < SRAM.length; i++) {
// 				if (SRAM[i].key == key)  {
// 					SRAM[i].data[0][1] = reducer(SRAM[i].data[0][1], data[key[0][1]]);
// 					break;
// 				}
// 			}
// 			if (i == SRAM.length) SRAM.push(new Partition(null, data[key], key));
// 		}
// 		if (++self.nShuffle < app.worker.length) return;
// 		self.partitions = SRAM;
// 		self.nextStage();
// 	};
// }

// function GroupedByKeyRDD(grid, app, job, param) {
// 	RDD.call(this, grid, app, job, param);

// 	this.shuffling = true;
// 	var self = this;
// 	var map = this.map = app.worker.map(function() {return {};});
// 	var SRAM = [];

// 	this.transform = function(data) {
// 		for (var i = 0; i < data.length; i++) {
// 			var key = data[i][0];
// 			var wid = ml.cksum(key) % app.worker.length;
// 			if (map[wid][key] === undefined)		// ATTENTION car la clé 1 et "1" ira dans la meme partition
// 				map[wid][key] = [[key, []]];
// 			map[wid][key][0][1].push(data[i][1]);
// 		}
// 	};

// 	this.shuffle = function(data) {
// 		for (var key in data) {
// 			for (var i = 0; i < SRAM.length; i++) {
// 				if (SRAM[i].key == key)  {
// 					SRAM[i].data = SRAM[i].data.concat(data[key][0][1]);
// 					break;
// 				}
// 			}
// 			if (i == SRAM.length) SRAM.push(new Partition(null, data[key], key));
// 		}
// 		if (++self.nShuffle < app.worker.length) return;
// 		self.partitions = SRAM;
// 		self.nextStage();
// 	};
// }

// function SampledRDD(grid, app, job, param) {
// 	RDD.call(this, grid, app, job, param);

// 	this.shuffling = true;
// 	var tmp = {}, SRAM = [];
// 	var withReplacement = param.args[0];
// 	var frac = param.args[1];
// 	var seed = param.args[2];

// 	this.transform = function(data) {
// 		if (tmp[p] === undefined) tmp[p] = [];
// 		for (var i = 0; i < data.length; i++) tmp[p].push(data[i]);
// 	};

// 	this.shuffle = function() {
// 		var p = 0;
// 		var rng = new ml.Random(seed);
// 		for (var i in tmp) {
// 			var L = Math.ceil(tmp[i].length * frac);
// 			var data = [];
// 			var idxVect = [];
// 			while (data.length != L) {
// 				var idx = Math.round(Math.abs(rng.next()) * (L - 1));
// 				if (!withReplacement && (idxVect.indexOf(idx) != -1)) continue;	// if already picked but no replacement mode
// 				idxVect.push(idx);
// 				data.push(tmp[i][idx]);
// 			}
// 			SRAM[p++] = new Partition(null, data);
// 		}
// 		this.partitions = SRAM;
// 		this.nextStage();
// 	};
// };

// this.sample = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var withReplacement = this.args[0];
// 	var frac = this.args[1];
// 	var seed = this.args[2];
// 	var tmp;

// 	this.reset = function() {
// 		this.SRAM = [];
// 		tmp = {};
// 	};

// 	this.reset();

// 	this.pipeline = function(array, p) {
// 		if (tmp[p] === undefined) tmp[p] = [];
// 		for (var i = 0; i < array.length; i++)
// 			tmp[p].push(array[i]);
// 	};

// 	this.tx_shuffle = function() {
// 		var p = 0;
// 		var rng = new ml.Random(seed);
// 		for (var i in tmp) {
// 			var L = Math.ceil(tmp[i].length * frac);
// 			var data = [];
// 			var idxVect = [];
// 			while (data.length != L) {
// 				var idx = Math.round(Math.abs(rng.next()) * (L - 1));
// 				if (!withReplacement && (idxVect.indexOf(idx) != -1))
// 					continue;	// if already picked but no replacement mode
// 				idxVect.push(idx);
// 				data.push(tmp[i][idx]);
// 			}
// 			this.SRAM[p++] = {data: data};
// 		}
// 		this.nShuffle = app.worker.length;	// No shuffle
// 	};
// };


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Old
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// 
// this.stream = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
//  	var self = this;
//  	var n = 0;
//  	var N = this.args[0];
//  	var streamIdx = this.args[1];

// 	app.completedStreams[streamIdx] = false;

// 	var onData = function(data, done) {			
// 		self.tmp = [data];
// 		if (self.persistent) self.save(0, self.tmp);
// 		self.pipelineToAction(0);
// 		if (++n == N) {
// 			n = 0;
// 			app.dones[streamIdx] = done;
// 			self.flush(false);
// 		} else done();
// 	}

// 	var onBlock = function(done) {
// 		app.dones[streamIdx] = done;
// 		self.flush(false);
// 	}

// 	var onEnd = function(done) {
// 		grid.removeListener(streamIdx, onData);
// 		grid.removeListener(streamIdx + '.block', onBlock);
// 		grid.removeListener(streamIdx + '.end', onEnd);
// 		app.completedStreams[streamIdx] = true;
// 		app.dones[streamIdx] = done;
// 		self.flush(true);
// 	}

// 	grid.on(streamIdx, onData);
// 	grid.on(streamIdx + '.block', onBlock);
// 	grid.on(streamIdx + '.end', onEnd);

// 	this.run = function () {;};
// };

// ------------------------------------------------------------------------------------ //
// Transformation
// ------------------------------------------------------------------------------------ //
// this.coGroup = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var SRAM, map;

// 	this.reset = function() {
// 		SRAM = this.SRAM = {};
// 		map = this.map = app.worker.map(function() {return {};});
// 	};

// 	this.reset();

// 	this.pipeline = function(array, p, from_id) {
// 		for (var i = 0; i < array.length; i++) {
// 			var key = array[i][0];
// 			var wid = ml.cksum(key) % app.worker.length;
// 			if (map[wid][key] === undefined) map[wid][key] = {key: key};
// 			if (map[wid][key][from_id] === undefined) map[wid][key][from_id] = [];
// 			map[wid][key][from_id].push(array[i][1]);
// 		}
// 	};

// 	this.shuffle = function(data) {
// 		var i, k;
// 		for (i in data)
// 			if (i in SRAM) {
// 				for (k in data[i]) {
// 					if (k == 'key') continue;
// 					if (SRAM[i][k] === undefined) SRAM[i][k] = data[i][k];
// 					else SRAM[i][k] = SRAM[i][k].concat(data[i][k]);
// 				}
// 			} else SRAM[i] = data[i];
// 		if (++this.nShuffle < app.worker.length) return;
// 		var res = [];
// 		for (i in SRAM) {
// 			var datasets = Object.keys(SRAM[i]);
// 			if (datasets.length != 3) continue;
// 			res.push({
// 				key: SRAM[i].key,
// 				data:[[SRAM[i].key, [SRAM[i][this.child[0].id], SRAM[i][this.child[1].id]]]]
// 			});
// 		}
// 		this.SRAM = res;
// 	};
// };

// this.join = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var type = this.args[1];
// 	var tmp, map;

// 	this.reset = function() {
// 		this.SRAM = [];
// 		tmp = {};
// 		tmp[this.child[0].id] = {};
// 		tmp[this.child[1].id] = {};
// 		map = this.map = app.worker.map(function() {return {};});
// 		for (var i = 0; i < app.worker.length; i++) {
// 			map[i][this.child[0].id] = {};
// 			map[i][this.child[1].id] = {};
// 		}
// 	};

// 	this.reset();

// 	this.pipeline = function(array, p, from_id) {
// 		for (var i = 0; i < array.length; i++) {
// 			var key = array[i][0];
// 			var str = JSON.stringify(key);
// 			var wid = ml.cksum(str) % app.worker.length;
// 			if (map[wid][from_id][key] === undefined)
// 				map[wid][from_id][key] = {key: key, data: []};
// 			map[wid][from_id][key].data.push(array[i][1]);
// 		}
// 	};

// 	this.shuffle = function(data) {
// 		var i, j, k, dataset;
// 		for (dataset in data) {
// 			if (tmp[dataset] === undefined) {
// 				tmp[dataset] = data[dataset];
// 				continue;
// 			}
// 			for (var key in data[dataset]) {
// 				if (tmp[dataset][key] === undefined) {
// 					tmp[dataset][key] = data[dataset][key];
// 					continue;
// 				}
// 				tmp[dataset][key].data = tmp[dataset][key].data.concat(data[dataset][key].data);
// 			}
// 		}
// 		if (++this.nShuffle < app.worker.length) return;

// 		var key0 = Object.keys(tmp[this.child[0].id]);
// 		var key1 = Object.keys(tmp[this.child[1].id]);

// 		switch (type) {
// 		case 'inner':
// 			for (i = 0; i < key0.length; i++) {
// 				var key = tmp[this.child[0].id][key0[i]].key;
// 				if (key1.indexOf(key0[i]) == -1) continue;
// 				var values = [tmp[this.child[0].id][key0[i]].data, tmp[this.child[1].id][key0[i]].data];				
// 				var tmp_data = [];
// 				for (j = 0; j < values[0].length; j++)
// 					for (k = 0; k < values[1].length; k++) {
// 						tmp_data.push([key, [values[0][j], values[1][k]]]);
// 				}
// 				this.SRAM.push({key: key, data: tmp_data});
// 			}
// 			break;
// 		case 'left':
// 			for (i = 0; i < key0.length; i++) {
// 				var key = tmp[this.child[0].id][key0[i]].key;
// 				if (key1.indexOf(key0[i]) == -1) {
// 					var values = tmp[this.child[0].id][key0[i]].data;
// 					data = [];
// 					for (j = 0; j < values.length; j++)
// 						data.push([key, [values[j], null]]);
// 				} else {
// 					var values = [tmp[this.child[0].id][key0[i]].data, tmp[this.child[1].id][key0[i]].data];
// 					data = [];
// 					for (j = 0; j < values[0].length; j++)
// 						for (k = 0; k < values[1].length; k++)
// 						data.push([key, [values[0][j], values[1][k]]]);						
// 				}
// 				this.SRAM.push({key: key, data: data});
// 			}
// 			break;
// 		case 'right':
// 			for (i = 0; i < key1.length; i++) {
// 				var key = tmp[this.child[1].id][key1[i]].key;
// 				if (key0.indexOf(key1[i]) == -1) {
// 					var values = tmp[this.child[1].id][key1[i]].data;
// 					data = [];
// 					for (j = 0; j < values.length; j++)
// 						data.push([key, [null, values[j]]]);
// 				} else {
// 					var values = [tmp[this.child[0].id][key1[i]].data, tmp[this.child[1].id][key1[i]].data];					
// 					data = [];
// 					for (j = 0; j < values[0].length; j++)
// 						for (k = 0; k < values[1].length; k++)
// 							data.push([key, [values[0][j], values[1][k]]]);
// 				}
// 				this.SRAM.push({key: key, data: data});
// 			}
// 			break;
// 		}
// 	};
// };

// this.crossProduct = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var left_dataset = this.child[0].id;
// 	var residentPartitions, shuffledPartitions, map, rxPartitions;

// 	this.reset = function() {
// 		this.SRAM = [];
// 		residentPartitions = [];
// 		shuffledPartitions = [];
// 		map = this.map = app.worker.map(function() {return shuffledPartitions;});
// 		rxPartitions = [];
// 	};

// 	this.reset();

// 	this.pipeline = function(array, p, from_id) {
// 		var dest = (from_id == left_dataset) ? residentPartitions : shuffledPartitions;
// 		if (dest[p] === undefined) dest[p] = [];
// 		for (var i = 0; i < array.length; i++) dest[p].push(array[i]);
// 	};

// 	function crossProduct(a, b) {
// 		var t0 = [];
// 		for (var i = 0; i < a.length; i++)
// 			for (var j = 0; j < b.length; j++)
// 				t0.push([a[i], b[j]]);
// 		return t0;
// 	}

// 	this.shuffle = function(data) {
// 		var i, j;
// 		for (i = 0; i < data.length; i++)
// 			rxPartitions.push(data[i]);

// 		if (++this.nShuffle < app.worker.length) return;
// 		for (j = 0; j < rxPartitions.length; j++) {
// 			for (i = 0; i < residentPartitions.length; i++)
// 				this.SRAM.push({data: crossProduct(residentPartitions[i], rxPartitions[j])});
// 		}
// 	};
// };

// this.intersection = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var tmp, map;

// 	this.reset = function() {
// 		this.SRAM = [];
// 		tmp = {};
// 		tmp[this.child[0].id] = [];
// 		tmp[this.child[1].id] = [];
// 		map = this.map = app.worker.map(function() {return {};});
// 		for (var i = 0; i < app.worker.length; i++) {
// 			map[i][this.child[0].id] = [];
// 			map[i][this.child[1].id] = [];
// 		}
// 	};

// 	this.reset();

// 	this.pipeline = function(array, p, from_id) {
// 		for (var i = 0; i < array.length; i++) {
// 			var str = JSON.stringify(array[i]);
// 			var wid = ml.cksum(str) % app.worker.length;
// 			map[wid][from_id].push(str);
// 		}
// 	};

// 	this.shuffle = function(data) {
// 		var i, j, k;
// 		for (i in data) tmp[i].push(data[i]);
// 		if (++this.nShuffle < app.worker.length) return;
// 		var result = [];
// 		for (i = 0; i < tmp[this.child[0].id].length; i++)
// 			loop:
// 			for (j = 0; j < tmp[this.child[0].id][i].length; j++) {
// 				var e = tmp[this.child[0].id][i][j];
// 				if (result.indexOf(e) != -1) continue;
// 				for (k = 0; k < tmp[this.child[1].id].length; k++)
// 					if (tmp[this.child[1].id][k].indexOf(e) != -1) {
// 						result.push(e);
// 						continue loop;
// 					}
// 			}
// 		this.SRAM = [{data: []}];
// 		for (i = 0; i < result.length; i++)
// 			this.SRAM[0].data.push(JSON.parse(result[i]));
// 	};
// };

// this.subtract = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var tmp, map;

// 	this.reset = function() {
// 		this.SRAM = [];
// 		tmp = {};
// 		tmp[this.child[0].id] = [];
// 		tmp[this.child[1].id] = [];
// 		map = this.map = app.worker.map(function() {return {};});
// 		for (var i = 0; i < app.worker.length; i++) {
// 			map[i][this.child[0].id] = [];
// 			map[i][this.child[1].id] = [];
// 		}
// 	};

// 	this.reset();

// 	this.pipeline = function(array, p, from_id) {
// 		for (var i = 0; i < array.length; i++) {
// 			var str = JSON.stringify(array[i]);
// 			var wid = ml.cksum(str) % app.worker.length;
// 			map[wid][from_id].push(str);
// 		}
// 	};

// 	this.shuffle = function(data) {
// 		var i;
// 		for (i in data) tmp[i] = tmp[i].concat(data[i]);
// 		if (++this.nShuffle < app.worker.length) return;
// 		var v1 = tmp[this.child[0].id];
// 		var v2 = tmp[this.child[1].id];
// 		var v_ref = [];
// 		for (i = 0; i < v1.length; i++) {
// 			//if (v_ref.indexOf(v1[i]) != -1) continue;
// 			if (v2.indexOf(v1[i]) != -1) continue;
// 			v_ref.push(v1[i]);
// 		}
// 		this.SRAM = [{data: v_ref.map(JSON.parse)}];
// 	};
// };

// this.partitionByKey = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var tmp, map;

// 	this.reset = function() {
// 		this.SRAM = [];
// 		tmp = {};
// 		map = this.map = app.worker.map(function() {return {};});
// 	};

// 	this.reset();

// 	this.pipeline = function(array) {
// 		for (var i = 0; i < array.length; i++) {
// 			var str = JSON.stringify(array[i][0]);
// 			var wid = ml.cksum(str) % app.worker.length;
// 			if (map[wid][str] === undefined)
// 				map[wid][str] = [];
// 			map[wid][str].push(array[i]);
// 		}
// 	};

// 	this.shuffle = function (data) {
// 		var key;
// 		for (key in data) {
// 			if (tmp[key] === undefined) tmp[key] = data[key];
// 			else tmp[key] = tmp[key].concat(data[key]);
// 		}
// 		if (++this.nShuffle < app.worker.length) return;
// 		for (key in tmp)
// 			this.SRAM.push({data: tmp[key], key: JSON.parse(key)});
// 	};
// };

// this.sortByKey = function(grid, app, job, node) {
// 	Transform.call(this, grid, app, job, node);
// 	var tmp, tmp2, keys, map;

// 	this.reset = function() {
// 		this.SRAM = [];
// 		tmp = {};
// 		tmp2 = {};
// 		keys = [];
// 		map = this.map = app.worker.map(function() {return tmp;});
// 	};

// 	this.reset();

// 	function split(a, n) {
// 		var len = a.length, out = [], i = 0;
// 		while (i < len) {
// 			var size = Math.ceil((len - i) / n--);
// 			out.push(a.slice(i, i += size));
// 		}
// 		return out;
// 	}

// 	this.pipeline = function(array) {
// 		for (var i = 0; i < array.length; i++) {
// 			if (tmp[array[i][0]] === undefined)
// 				tmp[array[i][0]] = {key: array[i][0], data: []};
// 			tmp[array[i][0]].data.push(array[i][1]);
// 		}
// 	};

// 	this.shuffle = function (data) {
// 		for (var key in data) {
// 			if (tmp2[key] === undefined) {
// 				tmp2[key] = data[key];
// 				keys.push(key);
// 				keys.sort();
// 			} else tmp2[key] = tmp2[key].concat(data[key]);
// 		}
// 		if (++this.nShuffle < app.worker.length) return;
// 		// Compute partition mapping over workers
// 		var mapping = split(keys, app.worker.length);
// 		for (var i = 0; i < mapping.length; i++) {
// 			if (app.worker[i].uuid != grid.host.uuid) continue;
// 			for (var j = 0; j < mapping[i].length; j++)
// 				this.SRAM.push({key: tmp2[mapping[i][j]].key, data: tmp2[mapping[i][j]].data.map(function(e) {
// 					return [tmp2[mapping[i][j]].key, e];
// 				})});
// 		}
// 	};
// };
