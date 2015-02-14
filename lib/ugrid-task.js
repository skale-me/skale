'use strict';

function treewalk(root, c_in, c_out) {
	var n = root;
	if (c_in) c_in(n);
	while (1) {
		if ((n.child.length > 0) && (++n.visits <= n.child.length)) {
			n = n.child[n.visits - 1];
			if (c_in) c_in(n);
		} else {
			n.visits = 0;
			if (c_out) c_out(n);
			if (n == root) break;
			n = n.anc;
		}
	}
}

function mapHDFSBlocks(blocks, workers) {
	if (blocks.length < workers.length)
		throw 'Temprorary error: blocks.length must be higher than worker.length'
	// Each block can be located on multiple slaves
	var mapping = {};
	for (var i = 0; i < workers.length; i++) {
		// WORKAROUND ICI, ipv6 pour les workers
		workers[i].ip = workers[i].ip.replace('::ffff:', '');
		if (mapping[workers[i].ip] == undefined)
			mapping[workers[i].ip] = {};
		mapping[workers[i].ip][i] = [];
	}
		
	// map each block to the least busy worker on same host
	for (var i = 0; i < blocks.length; i++) {
		for (var j = 0; j < blocks[i].host.length; j++) {
			if (mapping[blocks[i].host[j]] == undefined)
				throw 'block host ' + blocks[i].host[j] + ' unreachable'			
		}		
		var min_id = undefined;
		var host = '';
		// boucle sur les hosts du block
		for (var j = 0; j < blocks[i].host.length; j++) {
			for (w in mapping[blocks[i].host[j]]) {
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
		}
		// map block to min_id worker
		mapping[host][min_id].push({
			blockNum: blocks[i].blockNum,
			file: blocks[i].file,
			host: host
		});
	}
	var out = {};
	for (var h in mapping)
		for (var w in mapping[h])
			out[w] = mapping[h][w];
	return out;
}

// WARNING ==> hdfs works only for one hdfs query in DAG	
function preBuildTask(grid, worker, array, action, callback) {
	var hdfsTextFileNodes = [];
	treewalk(array, null, function(a) {
		if (a.transform == 'hdfsTextFile')
			hdfsTextFileNodes.push(a);
		// if an ancestor of node a is already in memory no need to trigger
		// the hdfs query one more time			
		if (a.inMemory)
			hdfsTextFileNodes = [];
	});
	// If no hdfs query needed, return immediatly
	if (hdfsTextFileNodes.length == 0) {
		callback();
		return;
	}
	var hdfsTextFile = hdfsTextFileNodes[0].args[0];
	
	grid.request_cb(worker[0], {cmd: 'hdfs', args: {file: hdfsTextFile}}, function (err, blocks) {
		if (err) throw err;
		var map = mapHDFSBlocks(blocks, worker);
		var args = [[]];
		for (var i = 0; i < worker.length; i++)
			args[0].push(map[i]);
		hdfsTextFileNodes[0].args = args;
		callback();
	});
}

function buildTask(grid, worker, array, action, callback) {
	preBuildTask(grid, worker, array, action, function() {
		var nStage = 0, lastStage = 0, num = 0, code = '';
		var task = new Array(worker.length);
		for (var i = 0; i < task.length; i++)
			task[i] = {node: {}};
		// ---------------------------------------------------------------------- //
		// Treewalk 1: Set stage index, node index and stage array
		// ---------------------------------------------------------------------- //
		treewalk(array, function(a) {
			if (a.transformType == 'wide') nStage++;
			if (lastStage < nStage) lastStage = nStage;
		}, function(a) {
			a.stageIdx = nStage;
			a.num = num++;
			if (a.transformType == 'wide') nStage--;
		});
		var stage = new Array(lastStage + 1);
		for (i = 0; i < stage.length; i++)
			stage[i] = {lineages: []};
		// ---------------------------------------------------------------------- //
		// Treewalk 2: Restore stage index, gen lineages code and set persistency
		// ---------------------------------------------------------------------- //
		treewalk(array, null, function(a) {
			var i, pipeline;
			// Set node args to be transmitted to workers
			for (i = 0; i < worker.length; i++)
				task[i].node[a.num] = a.getArgs(i);
			// Reverse stage index
			a.stageIdx = lastStage - a.stageIdx;
			// Add user defined tranformation to source code
			if (!a.inMemory && a.transformSource)
				code += a.transformSource();
			// Add specific shuffle action to source code
			if (a.transformType == 'wide')
				code += a.shuffleSource();
			pipeline = a.pipelineSource2() + '"PIPELINE_HERE"';
			if (a.inMemory) {
				// Reset current stage to start here,
				// TODO :  ==> AND DISCARD PREVIOUS STAGES !!!
				stage[a.stageIdx].lineages = [{
					input: a.inputFromRam(),
					pipeline: pipeline
				}];
			} else if (a.child.length === 0) {
				// If we leave a leaf, add a new lineage to current stage
				stage[a.stageIdx].lineages.push({
					input: a.inputSource(),
					pipeline: pipeline
				});
			} else if (stage[a.stageIdx].lineages.length === 0) {
				// If it's the begining of a new stage, create a new lineage
				stage[a.stageIdx].lineages = [{
					input: a.inputFromStageRam(),
					pipeline: pipeline
				}];
			} else 
				// Else append pipeline to current stage lineages
				for (i = 0; i < stage[a.stageIdx].lineages.length; i++)
					stage[a.stageIdx].lineages[i].pipeline = 
						stage[a.stageIdx].lineages[i].pipeline.replace('"PIPELINE_HERE"', pipeline);
			a.inMemory = a.persistent;
		});
		// ---------------------------------------------------------------------- //
		// Step 3: Write source code of Task() constructor
		// ---------------------------------------------------------------------- //
		for (i = 0; i < worker.length; i++)
			code += 'worker[' + i + '] = ' + JSON.stringify(worker[i]) + ';\n';
		for (i = 0; i < stage.length; i++) {
			code += 'stage_length[' + i + '] = ' + (stage[i].lineages.length || 1) + ';';
			code += 'stage_cnt[' + i + '] = 0;';
			code += 'stage_locked[' + i + '] = false;';
			code += 'stage[' + i + '] = function(callback) {\n';
			code += (i == lastStage) ? action.init : 'var res = {};\n';
			if (stage[i].lineages.length) {
				for (var j = 0; j < stage[i].lineages.length; j++) {
					var tmp = stage[i].lineages[j].pipeline;
					if (i == lastStage) 
						tmp = tmp.replace('"PIPELINE_HERE"', action.run + '"PIPELINE_HERE"');
					tmp = tmp.replace('"PIPELINE_HERE"', '');
					code += stage[i].lineages[j].input.replace('"PIPELINE_HERE"', tmp);
				}
			} else {
				function fromStageRam(sid) {
					var input = STAGE_RAM;
					for (var p in input) {
						for (var i = 0; i < input[p].length; i++) {
							var tmp = input[p][i];
							"PIPELINE_HERE"
						}
					}
					stage_cnt[sid]++;
				}
				var t0 = fromStageRam.toString() + '; fromStageRam(' + i + ');';
				code += t0.replace('"PIPELINE_HERE"', action.run);				
			}
			code += action.post || '';
			code += 'if (!stage_locked[' + i + '] && (stage_length[' + i + '] == stage_cnt[' + i + ']))';
			code += 'callback(res);';
			code += '}\n';
		}

		var taskCode = taskTemplate.toString().replace('"WORKERS_AND_STAGES"', code);
		require('fs').writeFileSync('/tmp/debug2.js', taskCode);
		// throw 'break';

		for (i = 0; i < worker.length; i++) {
			task[i].action = action;
			task[i].task = taskCode;
		}

		callback(task);
	});
}

var taskTemplate = function(grid, fs, Lines, ml, STAGE_RAM, RAM, msg) {
	var node = msg.data.args.node, action = msg.data.args.action;
	var stageIdx = 0, nShuffle = 0, finalCallback;
	var worker = [], stage = [], shuffle = [], transform = {};
	var stage_length = [], stage_cnt = [], stage_locked = [];

	"WORKERS_AND_STAGES"

	function shuffleRPC(host, args, callback) {
		grid.request_cb(host, {
			cmd: 'shuffle',
			args: args
		}, callback);
	}

	function runStage(idx) {
		stage[idx](function(res) {
			if (stageIdx == (stage.length - 1)) {
				STAGE_RAM = [];
				finalCallback(res);
			} else {				
				// Map partitions to workers
				var map = worker.map(function() {
					return {};
				});
				for (var p in res)
					map[p % worker.length][p] = res[p]; // Ok if partition name is a Number for now, use hashcoding later

				for (var i = 0; i < map.length; i++) {
					if (grid.host.uuid == worker[i].uuid) {
						nShuffle++;
						STAGE_RAM = map[i];
					} else {
						shuffleRPC(worker[i], map[i], function(err) {
							if (err) throw err;
						});
					}
				}
				// Run next stage if needed
				if (worker.length == 1) {
					nShuffle = 0;
					runStage(++stageIdx);
				}
			}
		});
	}

	this.run = function(callback) {
		finalCallback = callback;
		runStage(0);
	};

	this.processShuffle = function(msg) {
		shuffle[2]();	// id of stage to be shuffled
	};
};

module.exports.buildTask = buildTask;
