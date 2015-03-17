'use strict';

function treewalk(root, c_in, c_out) {
	var n = root;
	if (c_in) c_in(n);
	while (1)
		if (n.child.length && (++n.visits <= n.child.length)) {
			n = n.child[n.visits - 1];
			if (c_in) c_in(n);
		} else {
			n.visits = 0;
			if (c_out) c_out(n);
			if (n == root) break;
			n = n.anc;
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
	
	grid.request(worker[0], {cmd: 'hdfs', args: {file: hdfsTextFile}}, function (err, blocks) {
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
		var nStage = 0, lastStage = 0, num = 0;
		var task = new Array(worker.length);
		for (var i = 0; i < task.length; i++)
			task[i] = {node: {}};
		// ---------------------------------------------------------------------- //
		// Treewalk 1: Set stage index, node index and stage array
		// ---------------------------------------------------------------------- //
		treewalk(array, function(a) {
			if (a.dependency == 'wide') nStage++;
			if (lastStage < nStage) lastStage = nStage;
		}, function(a) {
			a.stageIdx = nStage;
			a.num = num++;
			if (a.dependency == 'wide') nStage--;
		});
		var stageData = {};
		for (i = 0; i < lastStage + 1; i++)
			stageData[i] = {lineages: []};
		// ---------------------------------------------------------------------- //
		// Treewalk 2: Reverse stage index, identify lineages and set persistency
		// ---------------------------------------------------------------------- //
		treewalk(array, null, function(a) {
			// Set node args to be transmitted to workers
			for (var i = 0; i < worker.length; i++)
				task[i].node[a.num] = a.getArgs(i);
			// Reverse stage index
			a.stageIdx = lastStage - a.stageIdx;
			if (a.dependency == 'wide') {
				stageData[a.stageIdx].shuffleType = a.transform;
				stageData[a.stageIdx].shuffleNum = a.num;
			}
			if (a.inMemory) {
				// if wide dependency discard stage up to this one and add a fromRAM to next stage
				if (a.dependency == 'wide') {
					for (var i = 0; i <= a.stageIdx; i++)
						delete stageData[i];
				} else {
					// if narrow dependency delete current stage lineages that lead to this transform
					// and add a fromRAM to this stage
					var lineagesToBeRemoved = [];
					for (var i = 0; i < stageData[a.stageIdx].lineages.length; i++) {
						var last = stageData[a.stageIdx].lineages[i].length - 1;
						for (var c = 0; c < a.child.length; c++)
							if (a.child[c].id == stageData[a.stageIdx].lineages[i][last].dest_id) {
								lineagesToBeRemoved.push(i);
								break;
							}
					}
					for (var i = 0; i < lineagesToBeRemoved.length; i++)
						stageData[a.stageIdx].lineages.splice(lineagesToBeRemoved[i], 1);
				}
				var sid = (a.dependency == 'wide') ? a.stageIdx + 1 : a.stageIdx;
				stageData[sid].lineages.push([{
					num: a.num,
					src_id: a.id,
					dest_id: a.id,
					type: 'fromRAM',
					dependency: a.dependency,
					persistent: a.persistent
				}]);
			} else if (a.child.length === 0) {
				// If transform is a leaf, add a new lineage to current stage
				stageData[a.stageIdx].lineages.push([{
					num: a.num,
					dest_id: a.id,
					type: a.transform,
					dependency: a.dependency,
					persistent: a.persistent
				}]);
			} else {
				// WARNING: Encore incomplet ici, il faut ajouter la tranformation
				// uniquement aux lineages menant à celle ci, il faut
				// faire le dual du travail de suppression dans le cas du inMemory
				// Else append pipeline to current stage lineages
				for (i = 0; i < stageData[a.stageIdx].lineages.length; i++) {
					var L = stageData[a.stageIdx].lineages[i].length;
					var src_id = stageData[a.stageIdx].lineages[i][L - 1].dest_id;
					stageData[a.stageIdx].lineages[i].push({
						num: a.num,
						dest_id: a.id,
						src_id: src_id,
						type: a.transform,
						dependency: a.dependency,
						persistent: a.persistent
					});
				}
				// If wide dependency add a fromStageRam lineage to next stage
				if (a.dependency == 'wide')
					stageData[a.stageIdx + 1].lineages.push([{
						num: a.num,
						dest_id: a.id,
						type: 'fromSTAGERAM',
						dependency: a.dependency,
						persistent: a.persistent
					}]);
			}
			a.inMemory = a.persistent;
		});

		var finalStageData = Object.keys(stageData).map(function (i) {return stageData[i]});
		finalStageData[finalStageData.length - 1].action = action;

		for (var i = 0; i < worker.length; i++) {
			task[i].workerData = worker;
			task[i].stageData = finalStageData;
		}
		callback(task);
	});
}

module.exports.buildTask = buildTask;
