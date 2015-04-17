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

module.exports.buildTask = function(grid, worker, array, action, callback) {
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

			// console.log(stageData[a.stageIdx].lineages)
			// console.log('id = ' + a.id)
			// console.log('child 0 ' + a.child[0].id)
			// console.log('child 1 ' + a.child[1].id)			

			for (i = 0; i < stageData[a.stageIdx].lineages.length; i++) {
				var L = stageData[a.stageIdx].lineages[i].length;
				// add transform only to lineage that leads to a
				var found = false;
				for (var c = 0; c < a.child.length; c++) {
					if (a.child[c].id == stageData[a.stageIdx].lineages[i][L - 1].dest_id) {
						found = true;
						break;
					}
				}
				// console.log('c = ' + c)
				// console.log('L = ' + L)
				if (!found) continue;				
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

	// console.log(stageData[0].lineages)

	var finalStageData = Object.keys(stageData).map(function (i) {return stageData[i]});
	finalStageData[finalStageData.length - 1].action = action;

	for (var i = 0; i < worker.length; i++) {
		task[i].workerData = worker;
		task[i].stageData = finalStageData;
	}
	callback(task);
}
