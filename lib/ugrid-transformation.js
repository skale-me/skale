'use strict';

var fs = require('fs');
var Connection = require('ssh2');

var ml = require('./ugrid-ml.js');
var trace = require('line-trace');
var Lines = require('./lines.js');
var sizeOf = require('../utils/sizeof.js');

var global = {require: require}

function MemoryManager() {
	var Kb = 1024;
	var Mb = 1024 * Kb;
	var Gb = 1024 * Mb;
	var MAX_MEMORY = 0.6 * Gb;
	var STORAGE_MEM_FRACTION = 0.4;
	var SHUFFLE_MEM_FRACTION = 0.3;
	var COLLECT_MEM_FRACTION = 0.2;	
	var maxStorageMemory = MAX_MEMORY * STORAGE_MEM_FRACTION;
	var maxShuffleMemory = MAX_MEMORY * SHUFFLE_MEM_FRACTION;
	var maxCollectMemory = MAX_MEMORY * COLLECT_MEM_FRACTION;	

	this.totalMemory = process.memoryUsage().rss;
	this.storageMemory = 0;
	this.shuffleMemory = 0;
	this.collectMemory = 0;

	this.storageFull = function() {return (this.storageMemory > maxStorageMemory);}
	this.shuffleFull = function() {return (this.shuffleMemory > maxShuffleMemory);}
	this.collectFull = function() {return (this.collectMemory > maxCollectMemory);}	
	this.report = function() {
		console.log('Config:');
		console.log('\t- max total = ' + Math.ceil(MAX_MEMORY / Mb) + ' Mb');
		console.log('\t- max storage = ' + Math.ceil(maxStorageMemory / Mb) + ' Mb');
		console.log('\t- max shuffle = ' + Math.ceil(maxShuffleMemory / Mb) + ' Mb');
		console.log('Usage:');
		console.log('\t- total = ' + Math.ceil(this.totalMemory / Mb) + ' Mb');
		console.log('\t- storage = ' + ((this.storageMemory / Mb) < 1 ? '<' : Math.ceil(this.storageMemory / Mb)) + ' Mb');
		console.log('\t- shuffle = ' + ((this.shuffleMemory / Mb) < 1 ? '<' : Math.ceil(this.shuffleMemory / Mb)) + ' Mb');
	}
}

var mm = new MemoryManager();

module.exports.UgridJob = function(grid, app, param) {
	this.id = param.jobId;
	this.sendResultSemaphore = 0;
	this.rdd = {};

	var stream = grid.createWriteStream(this.id, app.master_uuid), stageBoundaries = [], stages = [], self = this;

	// Step 1: Spawn RDDs from top to bottom
	for (var i = 0; i < param.node.length; i++) {
		if (app.rdd[param.node[i].id] == undefined) {
			switch (param.node[i].type) {
			// sources
			case 'parallelize': this.rdd[param.node[i].id] = new ParallelizedRDD(grid, app, this, param.node[i]); break;
			case 'randomSVMData': this.rdd[param.node[i].id] = new RandomSVMDataRDD(grid, app, this, param.node[i]); break;
			case 'textFile': this.rdd[param.node[i].id] = new TextFileRDD(grid, app, this, param.node[i]); break;
			case 'stream': this.rdd[param.node[i].id] = new StreamRDD(grid, app, this, param.node[i]); break;
			// narrow transform
			case 'map': this.rdd[param.node[i].id] = new MappedRDD(grid, app, this, param.node[i]); break;
			case 'union': this.rdd[param.node[i].id] = new UnionedRDD(grid, app, this, param.node[i]); break;
			case 'filter': this.rdd[param.node[i].id] = new FilteredRDD(grid, app, this, param.node[i]); break;
			case 'flatMap': this.rdd[param.node[i].id] = new FlatMappedRDD(grid, app, this, param.node[i]); break;
			case 'flatMapValues': this.rdd[param.node[i].id] = new FlatMappedValuesRDD(grid, app, this, param.node[i]); break;
			case 'mapValues': this.rdd[param.node[i].id] = new MappedValuesRDD(grid, app, this, param.node[i]); break;
			case 'sample': this.rdd[param.node[i].id] = new SampledRDD(grid, app, this, param.node[i]); break;
			// Wide transforms
			case 'distinct': this.rdd[param.node[i].id] = new DistinctRDD(grid, app, this, param.node[i]); break;
			case 'crossProduct': this.rdd[param.node[i].id] = new CrossProductRDD(grid, app, this, param.node[i]); break;
			case 'intersection': this.rdd[param.node[i].id] = new IntersectedRDD(grid, app, this, param.node[i]); break;
			case 'subtract': this.rdd[param.node[i].id] = new SubtractedRDD(grid, app, this, param.node[i]); break;
			// Key/Value wide transforms
			case 'reduceByKey': this.rdd[param.node[i].id] = new ReducedByKeyRDD(grid, app, this, param.node[i]); break;
			case 'groupByKey': this.rdd[param.node[i].id] = new GroupedByKeyRDD(grid, app, this, param.node[i]); break;
			case 'coGroup': this.rdd[param.node[i].id] = new CoGroupedRDD(grid, app, this, param.node[i]); break;
			case 'join': this.rdd[param.node[i].id] = new JoinedRDD(grid, app, this, param.node[i]); break;
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
		if (result != undefined) self.result = result;
		if ((app.wid != 0) && (++self.sendResultSemaphore != 2)) return;
		// if collect notify master that file is ready, else stream to master
		var o = {data: self.result};
		o.stream = (param.action.fun == 'collect');
		stream.write(o);
		// Notify next worker to send its results
		if (app.worker[app.wid + 1]) grid.send(app.worker[app.wid + 1].uuid, {cmd: 'action', jobId: self.id});
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

function RDD(grid, app, job, param) {
	var self = this;

	this.id = param.id;
	this.dependencies = param.dependencies;
	this.persistent = param.persistent;
	this.type = param.type;

	this.ugrid_dir = '/tmp/ugrid/';
	this.context_dir = this.ugrid_dir + app.contextId + '/';
	this.base_dir = this.context_dir + 'rdd_' + this.id + '/';
	this.worker_dir = this.base_dir + 'worker_' + app.wid + '/';
	this.job_dir = this.worker_dir + 'jobs/';	

	try {fs.mkdirSync(this.ugrid_dir)} catch (e) {};
	try {fs.mkdirSync(this.context_dir)} catch (e) {};
	try {fs.mkdirSync(this.base_dir)} catch (e) {};
	try {fs.mkdirSync(this.worker_dir)} catch (e) {};
	try {fs.mkdirSync(this.job_dir)} catch (e) {};

	this.collect = function(callback, param) {
		this.findPartitions(this);
		var result = '', p = -1;

		(function action() {
			if (++p < self.partitions.length) {
				self.pipeline(p, function processPartition() {process.nextTick(action)},
					function processElement(context, data) {
						var str = '';
						for (var i = 0; i < data.length; i++)
							str += JSON.stringify(data[i]) + '\n';
						mm.collectMemory += sizeOf(str);
						result += str;
						if (mm.collectFull()) {
							fs.appendFileSync(self.job_dir + job.id, result);
							mm.collectMemory = 0;
							result = '';
						}
					}
				);
			} else {
				fs.appendFileSync(self.job_dir + job.id, result);
				mm.collectMemory = 0;
				result = '';
				callback(self.job_dir + job.id);
			}
		})();
	};

	this.aggregate = function(callback, param) {
		this.findPartitions(this);
		var result = JSON.parse(JSON.stringify(param.args[0])), p = -1;
		var reducer = recompile(param.src);

		(function action() {
			if (++p < self.partitions.length) {
				self.pipeline(p, function() {process.nextTick(action)}, function (context, data) {
					for (var i = 0; i < data.length; i++) result = reducer(result, data[i]);
				});
			} else callback(result);
		})();
	};

	this.findPartitions = function (n) {
		if (n.partitions == undefined) {
			n.partitions = [];
			for (var i = 0; i < n.dependencies.length; i++) {
				var parentPartitions = self.findPartitions(job.rdd[n.dependencies[i]]);
				for (var p = 0; p < parentPartitions.length; p++)
					n.partitions.push(new Partition({id: n.dependencies[i], p: p}));
			}
		}
		return n.partitions;
	}

	this.pipeline = function(p, done, action) {
		var sourceRDD = self, sourcePartitionIdx = p, sourcePartition = self.partitions[p];
		var pipeline = action ? [{transform: action, p: sourcePartitionIdx}] : [];

		// Build p-th partition pipeline
		while ((sourcePartition.data == undefined) && sourcePartition.source) {
			if (sourceRDD.persistent && !sourceRDD.shuffling && !mm.storageFull()) {	// TO DEBUG partial storage RAM, add && (p == 0)
				sourcePartition.data = [];
				pipeline.unshift({transform: sourcePartition.save, p: sourcePartitionIdx});
			}
			if (sourceRDD.transform) {
				var item = {transform: sourceRDD.transform, p: sourcePartitionIdx};
				if (sourceRDD.shuffling) item.sourceId = sourcePartition.source.id;
				pipeline.unshift(item);
			}
			sourceRDD = job.rdd[sourcePartition.source.id];
			sourcePartitionIdx = sourcePartition.source.p;
			sourcePartition = sourceRDD.partitions[sourcePartitionIdx];
		}

		if (sourcePartition.data) sourcePartition.iterate(pipeline, done);
		else {
			// Si pas de parents
			if (sourceRDD.persistent  && !mm.storageFull()) {
				sourcePartition.data = [];
				pipeline.unshift({transform: sourcePartition.save, p: sourcePartitionIdx});
			}
			sourceRDD.iterate(sourcePartitionIdx, pipeline, done);
		}
	}

	if (this.persistent) app.rdd[param.id] = this;
}

function ShuffledRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var self = this;

	this.nShuffle = 0;
	this.shuffling = true;
	this.nextStage;
	this.shuffle_dir = this.worker_dir + 'shuffle/';
	this.post_shuffle_dir = this.worker_dir + 'postshuffle/';
	this.intermediate_dir = this.worker_dir + 'intermediate/';
	this.tmp_dir = this.worker_dir + 'tmp/';

	this.P = 1;							// default number of partitions
	this.preShuffleFiles = [];
	this.postShuffleFiles = [];
	this.intermediateFiles = [];

	this.run = function(nextStage) {
		this.findPartitions(this);		// find partitions
		this.nextStage = nextStage;		// nextStage() will be called elsewhere
		var p = -1;
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

	// Create RDD directory files
	try {fs.mkdirSync(this.tmp_dir)} catch (e) {};
	try {fs.mkdirSync(this.intermediate_dir)} catch (e) {};
	try {fs.mkdirSync(this.post_shuffle_dir)} catch (e) {};
	try {fs.mkdirSync(this.shuffle_dir)} catch (e) {};

	// Create shuffle files structure (one per worker)
	for (var to = 0; to < app.worker.length; to++) {
		this.preShuffleFiles[to] = {name: self.shuffle_dir + 'to_worker_' + to, buffer: ''};
		// create empty shuffle files
		try {fs.unlinkSync(this.preShuffleFiles[to].name);} catch (e){;}
		fs.appendFileSync(this.preShuffleFiles[to].name, '');
	}

	this.buffer = {};

	this.preShuffle = function() {self.spillToDisk('preshuffle', self.preShuffleFiles);}

	this.postShuffle = function() {
		// create intermediate files structure
		// NB: si on partitionByKey alors le nombre de partition n'est pas connu à l'instanciation du RDD
		for (var p = 0; p < self.P; p++) {
			self.intermediateFiles[p] = {name: self.intermediate_dir + 'p_' + p, buffer: ''};
			// Create empty intermediate files
			try {fs.unlinkSync(self.intermediateFiles[p].name);} catch (e){;}
			fs.appendFileSync(self.intermediateFiles[p].name, '');
		}

		// Population des fichiers de partitions à partir des fichiers de shuffles
		function populate(from, to, done) {
			var lines = new Lines();
			fs.createReadStream(self.postShuffleFiles[from]).pipe(lines);		// supprimer l'usage de lines ICI
			lines.on('data', function(line) {self.onShuffleData(JSON.parse(line))});
			lines.on('end', function() {
				if (++from < app.worker.length) populate(from, to, done);
				else {self.spillToDisk('postshuffle', self.intermediateFiles); done();}
			});
		}
		populate(0, app.wid, this.nextStage);
	}

	this.shuffle = function() {
		if (++this.nShuffle < app.worker.length) return;

		function fetchShuffleFile(from, done) {
	        var remote = self.base_dir + '/worker_' + from + '/shuffle/to_worker_' + app.wid;
	 		var local = self.base_dir + '/worker_' + app.wid + '/postshuffle/from_worker_' + from;

			if (grid.workerHost[from] == grid.workerHost[app.wid]) {
				self.postShuffleFiles[from] = remote;
	            if (++from < app.worker.length) fetchShuffleFile(from, done)
	            else done();
	            return;
			}

			app.transfer(grid.workerHost[from], remote, local, function (err, res) {
					self.postShuffleFiles[from] = local;
	            if (++from < app.worker.length) fetchShuffleFile(from, done)
	            else done(err, res);
			});
		}

		fetchShuffleFile(0, function() {
			self.partitions = [];
			for (var p = 0; p < self.P; p++)
				self.partitions.push(new Partition(null));
			self.postShuffle();
		});
	};

	this.spillToDisk = function(type, files) {
		// step 1: Prepare file buffers, flush global buffer, reset shuffleMemory
		if (type == 'preshuffle') {
			for (var hash in self.buffer) {
				var cksum = self.buffer[hash][0], input = self.buffer[hash][1];
				files[cksum % files.length].buffer += JSON.stringify([cksum, input]) + '\n';
			}
		} else if (type == 'postshuffle') {
			for (var hash in self.buffer) {
				var cksum = self.buffer[hash][0], input = self.buffer[hash][1];
				files[cksum % files.length].buffer += JSON.stringify(input) + '\n';
			}
		}
		self.buffer = {};
		mm.shuffleMemory = 0;
		// step 2: write files and flush file buffers
		for (var i = 0; i < files.length; i++) {
			fs.appendFileSync(files[i].name, files[i].buffer);
			files[i].buffer = '';
		}
	}

	this.iterate = function(p, pipeline, done) {
		var lines = new Lines();
		try {
			fs.createReadStream(self.intermediateFiles[p].name).pipe(lines);
			lines.on('data', function(line) {self.onIteratorData(pipeline, JSON.parse(line));});
			lines.on('end', function() {self.onIteratorEnd && self.onIteratorEnd(pipeline); done()});
		} catch (e) {done();}
	}
}

function recompile(s) {
	var args = s.match(/\(([^)]*)/)[1];
	var body = s.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
	return new Function(args, body);
}

function Partition(source, data, key) {
	this.source = source;		// {id: 1234, p: 0}
	this.data = data;
	this.key = key;				// peut-etre utile lors du partition by key
	var self = this;			// when class method is called with a different 'this'

	this.save = function(context, data) {
		if (mm.storageFull()) {				// Il faudrait supprimer du pipeline le save pour les partitions suivantes
			if (self.first == undefined) {
				self.first = true;
				console.log('STORAGE MEMORY IS FULL')
				console.log(process.memoryUsage())
			}
			self.data = undefined;	// delete partition as it does not fit in storage memory
			return data;
		}
		var size = sizeOf(data);
		mm.storageMemory += size;
		// If storage ok
		for (var i = 0; i < data.length; i++)
			self.data.push(data[i]);
		return data;
	}

	this.iterate = function(pipeline, done) {
		var buffer;
		for (var i = 0; i < self.data.length; i++) {
			buffer = [self.data[i]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	}
}

function StreamRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

 	var self = this;
 	var nPartitions = 1; 				// une seule partition pour le moment
 	var streamIdx = param.args[1];

	this.partitions = [];
	for (var p = 0; p < nPartitions; p++)
		this.partitions.push(new Partition(null));

	var onData = function(data, done) {
		var buffer = [data];
		for (var t = 0; t < self.partitionPipeline.length; t++)
			buffer = self.partitionPipeline[t].transform(self.partitionPipeline[t], buffer);
		done();
	}

	var onBlock = function(done) {
	// 	app.dones[streamIdx] = done;
	// 	self.flush(false);
	}

	var onEnd = function(done) {
		self.partitionDone();				// on débloque le done de la partition
		done();								// le done doit etre stocké et traité après traitement complet de la réponse
	}

	grid.on(streamIdx, onData);
	grid.on(streamIdx + '.block', onBlock);
	grid.on(streamIdx + '.end', onEnd);

	this.iterate = function(p, pipeline, done) {
		grid.send(app.master_uuid, {cmd: 'startStream', streamid: streamIdx, jobid: job.id});
		self.partitionDone = done;
		self.partitionPipeline = pipeline;
	};
};

function ParallelizedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);

	this.partitions = [];
	var partitions = param.args || [];
	for (var p = 0; p < partitions.length; p++)
		this.partitions.push(new Partition(null, partitions[p]));
}

function RandomSVMDataRDD(grid, app, job, param) {
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
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
		done();
	}
}

function TextFileRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var self = this, blocks = [];

	for (var i = 0; i < param.args[2].length; i++)
		if (param.args[2][i].wid == app.wid) blocks.push(param.args[2][i]);

	this.partitions = [];
	for (var p = 0; p < blocks.length; p++)
		this.partitions.push(new Partition(null));

	function sendFirstLine(block, line) {
		grid.send(app.worker[param.args[2][block.bid - 1].wid].uuid, {
			cmd: job.id + '.' + self.id + '.firstline', 
			data: {firstline: line, bid: (block.bid - 1)}
		});
		block.firstline_sent = true;
	}

	// Set firstline event listener for each block except the last one
	for (var i = 0; i < blocks.length; i++) {
		if (blocks[i].bid == (param.args[2].length - 1)) continue;
		grid.once(job.id + '.' + self.id + '.firstline', function (data) {
			var block = param.args[2][data.data.bid];
			block.lastline_received = true;
			if (block.lastline) block.lastline += data.data.firstline;
			else block.lastline = data.data.firstline;
			if (block.lastline_reached) block.finish();
		});
	}

	this.iterate = function(p, pipeline, done) {
		var chunk_buffer = '', buffer, block = blocks[p];
		var rs = fs.createReadStream(block.file, block.opt);

		block.finish = function () {
			var lines = block.lastline.split(/\r\n|\r|\n/);
			if (block.firstline_sent) processLine(lines[0]);
			else sendFirstLine(block, lines[0]);
			for (var i = 1; i < lines.length; i++) processLine(lines[i]);
			done();
		}

		var processChunkOnce = function(chunk) {
			var lines = (chunk_buffer + chunk).split(/\r\n|\r|\n/);						// Construction du vecteur de lignes
			chunk_buffer = lines.pop();													// Extraction de la dernière ligne incomplète			
			if (lines.length) {															// si il y a au moins 2 lignes
				sendFirstLine(block, lines[0]);											// envoie de la première ligne au block precedent
				for (var i = 1; i < lines.length; ++i) processLine(lines[i]);			// traitement des lignes suivantes				
				rs.on('data', processChunk);											// passage au régime établi
			} else rs.once('data', processChunkOnce);									// sinon on attend la complétion de la première ligne	
		}

		rs.on('end', function() {
			if (block.bid == 0) block.firstline_sent = true;
			if (block.bid == param.args[2].length - 1) block.lastline_received = true;
			block.lastline_reached = true;
			if (block.lastline) block.lastline = chunk_buffer + block.lastline;
			else block.lastline = chunk_buffer;
			if (block.lastline_received) block.finish();
		});

		var processChunk = function(chunk) {
			var lines = (chunk_buffer + chunk).split(/\r\n|\r|\n/);
			chunk_buffer = lines.pop();
			for (var i = 0; i < lines.length; ++i) processLine(lines[i]);
		}

		function processLine(line) {
			buffer = [line];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}

		if (block.bid) rs.once('data', processChunkOnce);
		else rs.on('data', processChunk);
	}
}
// ---------------------------------------------------------------------------------------------------- //
// Narrow Transformations
// ---------------------------------------------------------------------------------------------------- //
function MappedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var mapper = recompile(param.src);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			tmp[i] = mapper(data[i], param.args, global);
		return tmp;
	}
}

function FlatMappedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var mapper = recompile(param.src);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) tmp = tmp.concat(mapper(data[i], param.args, global));
		return tmp;
	}
}

function MappedValuesRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var mapper = recompile(param.src);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) tmp[i] = [data[i][0], mapper(data[i][1], param.args, global)];
		return tmp;
	}
}

function FlatMappedValuesRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var mapper = recompile(param.src);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++) {
			var t0 = mapper(data[i][1], param.args, global);
			tmp = tmp.concat(t0.map(function(e) {return [data[i][0], e];}));
		}
		return tmp;
	}
}

function FilteredRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var filter = recompile(param.src);

	this.transform = function(context, data) {
		var tmp = [];
		for (var i = 0; i < data.length; i++)
			if (filter(data[i], param.args, global)) tmp.push(data[i]);
		return tmp;
	}
}

function UnionedRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
}

function SampledRDD(grid, app, job, param) {
	RDD.call(this, grid, app, job, param);
	var withReplacement = param.args[0];
	var frac = param.args[1];
	var seed = param.args[2];
	var rng = withReplacement ? new ml.Poisson(frac, seed) : new ml.Random(seed);

	this.transform = function(context, data) {
		var tmp = [];
		if (withReplacement) {
			for (var i = 0; i < data.length; i++)
				for (var j = 0; j < rng.sample(); j++)
					tmp.push(data[i]);
		} else {
			for (var i = 0; i < data.length; i++)
				if (rng.nextDouble() < frac) tmp[i] = data[i];
		}
		return tmp;
	}
}

function DistinctRDD(grid, app, job, param) {
	ShuffledRDD.call(this, grid, app, job, param);
	var self = this;

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var str = JSON.stringify(data[i]);
			if (self.buffer[str] == undefined) {
				self.buffer[str] = [ml.cksum(str), [data[i]]];
				mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
			}
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	};

	this.onShuffleData = function(data) {
		var cksum = data[0], input = data[1][0], str = JSON.stringify(input);
		if (self.buffer[str] == undefined) {
			self.buffer[str] = [cksum, [input]];
			mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
		}
		if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
	}

	self.index = [];		// à remplacer par self.buffer

	this.onIteratorData = function(pipeline, data) {
		var input = data[0], str = JSON.stringify(input), value = data[1];
		if (self.index.indexOf(str) != -1) return;
		self.index.push(str);
		var buffer = [input];
		for (var t = 0; t < pipeline.length; t++)
			buffer = pipeline[t].transform(pipeline[t], buffer);
	}
};

function CrossProductRDD(grid, app, job, param) {
	ShuffledRDD.call(this, grid, app, job, param);
	var self = this;

	this.secondaryBuffer = {};
	this.tmpFile = {name: self.tmp_dir + 'left', buffer: ''};
	try {fs.unlinkSync(self.tmpFile.name);} catch (e){;}
	fs.appendFileSync(self.tmpFile.name, '');

	this.transform = function(context, data) {
		var sid = (context.sourceId == self.dependencies[0]) ? 0 : 1;

		if (sid == 0) {			// premier dataset
			for (var i = 0; i < data.length; i++) {
				var str = JSON.stringify(data[i]);
				if (self.secondaryBuffer[str] == undefined) {
					self.secondaryBuffer[str] = [ml.cksum(str), [data[i]]];
					mm.shuffleMemory += sizeOf(self.secondaryBuffer[str]) + sizeOf(str);
					if (mm.shuffleFull()) self.spillSecondaryBufferToDisk(self.tmpFile);
				} else {
					self.secondaryBuffer[str][1].push(data[i]);
					mm.shuffleMemory += sizeOf(str);
					if (mm.shuffleFull()) self.spillSecondaryBufferToDisk(self.tmpFile);
				}
			}
		} else {				// second dataset
			for (var i = 0; i < data.length; i++) {
				var str = JSON.stringify(data[i]);
				if (self.buffer[str] == undefined) {
					self.buffer[str] = [ml.cksum(str), [data[i]]];
					mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
					if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
				} else {
					self.buffer[str][1].push(data[i]);
					mm.shuffleMemory += sizeOf(str);
					if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
				}
			}
		}
	};

	this.spillToDisk = function(type, files) {
		// step 1: Prepare file buffers, flush global buffer, reset shuffleMemory
		if (type == 'preshuffle') {
			for (var hash in self.buffer) {
				var input = self.buffer[hash][1];
				for (var j = 0; j < input.length; j++) {
					for (var i = 0; i < files.length; i++) {
						files[i].buffer += JSON.stringify([cksum, [input[j]]]) + '\n';
					}
				}
			}
		} else if (type == 'postshuffle') {
			for (var hash in self.buffer) {
				var cksum = self.buffer[hash][0], input = self.buffer[hash][1];
				for (var j = 0; j < input.length; j++)
					files[cksum % files.length].buffer += JSON.stringify([input[j]]) + '\n';
			}
		}
		self.buffer = {};
		mm.shuffleMemory = 0;
		// step 2: write files and flush file buffers
		for (var i = 0; i < files.length; i++) {
			fs.appendFileSync(files[i].name, files[i].buffer);
			files[i].buffer = '';
		}
	}

	this.spillSecondaryBufferToDisk = function(file) {
		// step 1: Prepare file buffers, flush global buffer, reset shuffleMemory
		for (var hash in self.secondaryBuffer) {
			var cksum = self.secondaryBuffer[hash][0], input = self.secondaryBuffer[hash][1];
			for (var j = 0; j < input.length; j++) {
				file.buffer += JSON.stringify([cksum, [input[j]]]) + '\n';
			}
		}
		self.secondaryBuffer = {};
		mm.shuffleMemory = 0;							// ERROR: ici il faut retrancher uniquement la portion du secondary buffer
		// step 2: write files and flush file buffers
		fs.appendFileSync(file.name, file.buffer);
		file.buffer = '';
	}

	this.preShuffle = function() {
		self.spillToDisk('preshuffle', self.preShuffleFiles);
		self.spillSecondaryBufferToDisk(self.tmpFile);
	}

	// lors du shuffle pour chaque entrée on boucle sur le fichier de gauche
	// et in génère les paires de valeur
	this.postShuffle = function() {
		for (var p = 0; p < self.P; p++) {
			self.intermediateFiles[p] = {name: self.intermediate_dir + 'p_' + p, buffer: ''};
			// Create empty intermediate files
			try {fs.unlinkSync(self.intermediateFiles[p].name);} catch (e){;}
			fs.appendFileSync(self.intermediateFiles[p].name, '');
		}

		// Population des fichiers de partitions à partir des fichiers de shuffles
		function populate(from, done) {
			var lines = new Lines(), left_lines = new Lines();
			fs.createReadStream(self.postShuffleFiles[from]).pipe(lines);		// supprimer l'usage de lines ICI
			fs.createReadStream(self.tmpFile.name).pipe(left_lines);		// supprimer l'usage de lines ICI

			lines.on('data', function(line) {
				var right_data = JSON.parse(line);
				var cksum = right_data[0];
				left_lines.on('data', function(line) {
					var left_data = JSON.parse(line);
					var tmp = [left_data[1][0], right_data[1][0]];
					var str = JSON.stringify(tmp);
					if (self.buffer[str] == undefined) {
						self.buffer[str] = [cksum, [tmp]];
						mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
					} else {
						self.buffer[str][1].push(tmp);
						mm.shuffleMemory += sizeOf(str);
					}
					if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
				});
			});
			lines.on('end', function() {
				if (++from < app.worker.length) populate(from, done);
				else {self.spillToDisk('postshuffle', self.intermediateFiles); done();}
			});
		}
		populate(0, this.nextStage);
	}

	this.onIteratorData = function(pipeline, data) {
		var input = data[0];
		var buffer = [input];
		for (var t = 0; t < pipeline.length; t++)
			buffer = pipeline[t].transform(pipeline[t], buffer);
	}
}

function IntersectedRDD(grid, app, job, param) {
	ShuffledRDD.call(this, grid, app, job, param);
	var self = this;

	this.transform = function(context, data) {
		var sid = (context.sourceId == self.dependencies[0]) ? 0 : 1;
		for (var i = 0; i < data.length; i++) {
			var str = JSON.stringify(data[i]);
			if (self.buffer[str] == undefined) {
				self.buffer[str] = [ml.cksum(str), [data[i], [0, 0]]];
				mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
			}
			self.buffer[str][1][1][sid]++;
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	};

	this.onShuffleData = function(data) {
		var cksum = data[0], input = data[1][0], str = JSON.stringify(input), value = data[1][1];
		if (self.buffer[str] == undefined) {
			self.buffer[str] = [cksum, [input, [0, 0]]];
			mm.shuffleMemory += sizeOf(self.buffer[str]) + sizeOf(str);
		}
		self.buffer[str][1][1][0] += value[0];
		self.buffer[str][1][1][1] += value[1];
		if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
	}

	this.onIteratorData = function(pipeline, data) {
		var input = data[0], str = JSON.stringify(input), value = data[1];
		if (self.buffer[str] == undefined) self.buffer[str] = [input, [0, 0]];
		self.buffer[str][1][0] += value[0];
		self.buffer[str][1][1] += value[1];
	}

	this.onIteratorEnd = function(pipeline) {
		for (var str in self.buffer) {
			if (self.buffer[str][1][0] && self.buffer[str][1][1]) {
				var buffer = [self.buffer[str][0]];
				for (var t = 0; t < pipeline.length; t++)
					buffer = pipeline[t].transform(pipeline[t], buffer);
			}
		}
	}
}

function SubtractedRDD(grid, app, job, param) {
	IntersectedRDD.call(this, grid, app, job, param);
	var self = this;

	this.onIteratorEnd = function(pipeline) {
		for (var str in self.buffer) {
			if (self.buffer[str][1][0] && !self.buffer[str][1][1]) {
				for (var i = 0; i < self.buffer[str][1][0]; i++) {
					var buffer = [self.buffer[str][0]];
					for (var t = 0; t < pipeline.length; t++)
						buffer = pipeline[t].transform(pipeline[t], buffer);
				}
			}
		}
	}
}

// ------------------------------------------------------------------------ //
// PairRDD transformations
// ------------------------------------------------------------------------ //
function CombinedByKeyRDD(grid, app, job, param) {
	ShuffledRDD.call(this, grid, app, job, param);
	var self = this;

	this.transform = function(context, data) {
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0], value = data[i][1], str = JSON.stringify(key);
			if (self.buffer[str] == undefined)
				self.buffer[str] = [ml.cksum(str), [key, JSON.parse(JSON.stringify(self.init))]];
			mm.shuffleMemory -= sizeOf(self.buffer[str][1][1]);
			self.buffer[str][1][1] = self.reducer(self.buffer[str][1][1], value, self.reducerArgs, global);
			mm.shuffleMemory += sizeOf(self.buffer[str][1][1]);
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	};

	this.onShuffleData = function(data) {
		var cksum = data[0], input = data[1], key = input[0], value = input[1];
		if (self.buffer[key] == undefined) self.buffer[key] = [cksum, input];
		else {
			mm.shuffleMemory -= sizeOf(self.buffer[key][1][1]);
			self.buffer[key][1][1] = self.combiner(self.buffer[key][1][1], value, self.combinerArgs, global);
			mm.shuffleMemory += sizeOf(self.buffer[key][1][1]);
		}
		if (mm.shuffleFull()) self.spillToDisk('postshuffle', self.intermediateFiles);
	}

	this.onIteratorData = function(pipeline, data) {
		var key = data[0], value = data[1];
		if (self.buffer[key] == undefined) self.buffer[key] = data;
		else self.buffer[str][1] = self.combiner(self.buffer[str][1], value, self.combinerArgs, global);
	}

	this.onIteratorEnd = function(pipeline) {
		for (var key in self.buffer) {
			var buffer = [self.buffer[key]];
			for (var t = 0; t < pipeline.length; t++)
				buffer = pipeline[t].transform(pipeline[t], buffer);
		}
	}
}

function ReducedByKeyRDD(grid, app, job, param) {
	CombinedByKeyRDD.call(this, grid, app, job, param);

	this.init = param.args[0];
	this.reducerArgs = param.args[1];
	this.combinerArgs = param.args[1];	
	this.reducer = this.combiner = recompile(param.src);
}

function GroupedByKeyRDD(grid, app, job, param) {
	CombinedByKeyRDD.call(this, grid, app, job, param);

	this.init = [];
	this.reducer = function(a, b) {
		a.push(b);
		return a;
	}
	this.combiner = function(a, b) {
		return a.concat(b);
	}
}

function CoGroupedRDD(grid, app, job, param) {
	CombinedByKeyRDD.call(this, grid, app, job, param);
	var self = this;

	this.init = [[], []];
	this.combiner = function(a, b) {
		a[0] = a[0].concat(b[0]);
		a[1] = a[1].concat(b[1]);
		return a;
	}

	this.transform = function(context, data) {
		var sid = (context.sourceId == self.dependencies[0]) ? 0 : 1;
		for (var i = 0; i < data.length; i++) {
			var key = data[i][0], value = data[i][1], str = JSON.stringify(key);
			if (self.buffer[str] == undefined)
				self.buffer[str] = [ml.cksum(str), [JSON.parse(key), JSON.parse(JSON.stringify(self.init))]];
			self.buffer[str][1][1][sid].push(value);
			try {mm.shuffleMemory += sizeOf(value)}
			catch(err) {console.log(data)}
			if (mm.shuffleFull()) self.spillToDisk('preshuffle', self.preShuffleFiles);
		}
	}
}

function JoinedRDD(grid, app, job, param) {
	CoGroupedRDD.call(this, grid, app, job, param);
	var self = this, type = param.args[1];

	this.onIteratorEnd = function(pipeline) {
		for (var key in self.buffer) {
			var acc = self.buffer[key][1];
			switch(type) {
			case 'inner':
				if (acc[0].length && acc[1].length) {
					for (var i = 0; i < acc[0].length; i++) {
						for (var j = 0; j < acc[1].length; j++) {
							var buffer = [[JSON.parse(key), [acc[0][i], acc[1][j]]]];
							for (var t = 0; t < pipeline.length; t++)
								buffer = pipeline[t].transform(pipeline[t], buffer);
						}
					}
				}
				break;
			case 'left':
				for (var i = 0; i < acc[0].length; i++) {
					if (acc[1].length) {
						for (var j = 0; j < acc[1].length; j++) {
							var buffer = [[JSON.parse(key), [acc[0][i], acc[1][j]]]];
							for (var t = 0; t < pipeline.length; t++)
								buffer = pipeline[t].transform(pipeline[t], buffer);
						}
					} else {
						var buffer = [[JSON.parse(key), [acc[0][i], null]]];
						for (var t = 0; t < pipeline.length; t++)
							buffer = pipeline[t].transform(pipeline[t], buffer);
					}
				}
				break;
			case 'right':
				for (var j = 0; j < acc[1].length; j++) {
					if (acc[0].length) {
						for (var i = 0; i < acc[0].length; i++) {
							var buffer = [[JSON.parse(key), [acc[0][i], acc[1][j]]]];
							for (var t = 0; t < pipeline.length; t++)
								buffer = pipeline[t].transform(pipeline[t], buffer);
						}
					} else {
						var buffer = [[JSON.parse(key), [null, acc[1][j]]]];
						for (var t = 0; t < pipeline.length; t++)
							buffer = pipeline[t].transform(pipeline[t], buffer);
					}
				}
				break;
			}
		}
	}
}