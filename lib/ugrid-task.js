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
		var stageData = new Array(lastStage + 1);
		for (i = 0; i < stageData.length; i++) 
			stageData[i] = {lineages: []};
		// ---------------------------------------------------------------------- //
		// Treewalk 2: Restore stage index, gen lineages and set persistency
		// ---------------------------------------------------------------------- //
		treewalk(array, null, function(a) {
			var i;
			// Set node args to be transmitted to workers
			for (i = 0; i < worker.length; i++)
				task[i].node[a.num] = a.getArgs(i);
			// Reverse stage index
			a.stageIdx = lastStage - a.stageIdx;
			if (a.dependency == 'wide') {
				stageData[a.stageIdx].shuffleType = a.transform;
				stageData[a.stageIdx].shuffleNum = a.num;
			}
			if (a.inMemory) {
				// Reset current stage to start here, TODO : DISCARD PREVIOUS STAGES !!!
				stageData[a.stageIdx].lineages = [[{
					num: a.num,
					id: a.id,
					type: 'fromRAM',
					persistent: a.persistent
				}]];
			} else if (a.child.length === 0) {
				// If we leave a leaf, add a new lineage to current stage
				stageData[a.stageIdx].lineages.push([{
					num: a.num,
					id: a.id,
					type: a.transform,
					persistent: a.persistent
				}]);
			} else if (stageData[a.stageIdx].lineages.length === 0) {
				stageData[a.stageIdx].lineages = [[{
					num: a.num,
					id: a.id,
					type: 'fromSTAGERAM',
					persistent: a.persistent
				}, {
					num: a.num,
					id: a.id,
					type: a.transform,
					persistent: a.persistent
				}]];
			} else 
				// Else append pipeline to current stage lineages
				for (i = 0; i < stageData[a.stageIdx].lineages.length; i++) {
					stageData[a.stageIdx].lineages[i].push({
						num: a.num,
						id: a.id,
						type: a.transform,
						persistent: a.persistent
					})
				}
			a.inMemory = a.persistent;
		});

		for (i = 0; i < worker.length; i++) {
			task[i].workerData = worker;
			if (stageData[stageData.length - 1].lineages.length == 0)
				stageData[stageData.length - 1].lineages = [[{type: 'fromSTAGERAM'}]]
			task[i].stageData = stageData;
			stageData[stageData.length - 1].action = action;			
		}		
		callback(task);
	});
}

module.exports.buildTask = buildTask;
