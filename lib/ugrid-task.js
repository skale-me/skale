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

function buildTask(worker, array, action) {
	var node = {}, nStage = 0, lastStage = 0, num = 0, code = '';
	var task = new Array(worker.length);
	for (var i = 0; i < task.length; i++)
		task[i] = {node: {}};
	// ---------------------------------------------------------------------- //
	// Treewalk 1: Set stage index, node index and stage array
	// ---------------------------------------------------------------------- //
	// var stage = [{lineages: []}];
	treewalk(array, function (a) {
		if (a.transformType == 'wide') nStage++;
		if (lastStage < nStage) lastStage = nStage;
	}, function (a) {
		a.stageIdx = nStage;
		a.num = num++;
		if (a.transformType == 'wide') {
			nStage--;
			// stage.push = {lineages: []};
		}
	});
	var stage = new Array(lastStage + 1);
	for (var i = 0; i < stage.length; i++)
		stage[i] = {lineages: []}
	// ---------------------------------------------------------------------- //
	// Treewalk 2: Restore stage index, gen lineages code and set persistency
	// ---------------------------------------------------------------------- //
	treewalk(array, null, function (a) {
		//////////////////////////////////////////////////////////////////////
		// NB Move the argument mapper to ugrid-array.js
		for (var i = 0; i < worker.length; i++) {
			if (a.transform == 'parallelize') {
				task[i].node[a.num] = {args: [[a.args[0][i]], a.args[1]]}
			} else if (a.transform == 'randomSVMData') {
				task[i].node[a.num] = {args: [[a.args[0][i]], a.args[1], a.args[2][i], a.args[3]]}
			} else if (a.transform == 'textFile') {
				task[i].node[a.num] = {args: [a.args[0], a.args[1][i], a.args[2], a.args[3]]}
			} else task[i].node[a.num] = {args: a.args}
		}
		//////////////////////////////////////////////////////////////////////
		// reverse stage index
		a.stageIdx = lastStage - a.stageIdx;
		// If we leave an inMemory array, reset current stage to start here,
		// TODO: discard previous stages
		if (a.inMemory) {
			var tmp = (a.stageIdx && lastStage) ?
				'var input = STAGE_RAM;\n' : 'var input = RAM[' + a.id + '];\n';
			stage[a.stageIdx].lineages = [{input: tmp, pipeline: ''}];
		} else if (a.child.length == 0) {
			// If we leave a leaf, add a new lineage to current stage
			var tmp = a.inputSource();
			if (a.persistent) tmp += 'RAM[' + a.id + '] = input;\n';
			stage[a.stageIdx].lineages.push({input: tmp, pipeline: ''});
		} else {
			var tmp = a.pipelineSource();
			code += a.transformSource();
			if (a.persistent && (a.transformType == 'narrow'))
				tmp += '\t\tRAM[' + a.id + '][p].push(tmp);\n';
			// If it's the begining of a new stage, create a new lineage
			if (stage[a.stageIdx].lineages.length == 0)
				stage[a.stageIdx].lineages = [{input: 'var input = STAGE_RAM;\n', pipeline: tmp}];
			// Else append pipeline to current stage lineages
			else for (var i = 0; i < stage[a.stageIdx].lineages.length; i++)
					stage[a.stageIdx].lineages[i].pipeline += tmp;
		}
		// Set persistency for next build
		a.inMemory = a.persistent;
	});

	// ---------------------------------------------------------------------- //
	// Step 3: Write source code of Task() constructor
	// ---------------------------------------------------------------------- //
	for (var i = 0; i < worker.length; i++)
		code += 'worker[' + i + '] = ' + JSON.stringify(worker[i]) + ';\n';
	for (var i = 0; i < stage.length; i++) {
		code += 'stage[' + i + '] = function() {\n';
		code += (i == lastStage) ? action.init : 'var res = {};\n';
		for (var j = 0; j < stage[i].lineages.length; j++) {
			code += stage[i].lineages[j].input;
			code += 'for (var p in input) {\n' +
				'\tfor (var i = 0; i < input[p].length; i++) {\n' +
				// '\tfor (var i in input[p]) {\n' +
				'\t\tvar tmp = input[p][i];\n';
			code += stage[i].lineages[j].pipeline;
			if (i == lastStage)
				code += action.run;
			code += '\t}\n}\n';
		}
		code += action.post || '';
		code += 'return res;\n}\n';
	}

	var taskCode = taskTemplate.toString().replace('"WORKERS_AND_STAGES"', code);

	for (var i = 0; i < worker.length; i++) {
		task[i].action = action;
		task[i].task = taskCode;
	}

	return task;
}

var taskTemplate = function(fs, grid, ml, STAGE_RAM, RAM, node, action) {	
	var stageIdx = 0, nShuffle = 0, finalCallback;
	var worker = [], stage = [], shuffle = [], transform = {};

	"WORKERS_AND_STAGES"

	function shuffleRPC(host, args, callback) {
		grid.request_cb(host, {cmd: 'shuffle', args: args}, callback);
	}

	function runStage(idx) {
		var lastStage = (stageIdx == (stage.length - 1));
		var res = stage[idx]();

		if (lastStage) {
			STAGE_RAM = [];
			finalCallback(res);
		} else {
			// Map partitions to workers			
			var map = worker.map(function(n) {return {};});
			for (var p in res)				
				map[p % worker.length][p] = res[p]; // Ok if partition name is a Number for now, use hashcoding later

			for (var i = 0; i < map.length; i++) {
				if (grid.host.uuid == worker[i].uuid) {
					nShuffle++;
					STAGE_RAM = map[i];
				} else
					shuffleRPC(worker[i], map[i], function(err, res) {if (err) throw err;});
			}
			// Run next stage if needed
			if (worker.length == 1) {
				nShuffle = 0;
				runStage(++stageIdx);
			}
		}
	}

	this.run = function(callback) {
		finalCallback = callback;
		runStage(0);
	}

	this.processShuffle = function(msg) {
		nShuffle++;		
		var data = msg.data.args;
		// Reduce intermediates results (to be generated programmatically)
		for (p in data) {
			if (!STAGE_RAM[p] || !data[p]) continue;
			for (var i = 0; i < STAGE_RAM[p][0].acc.length; i++)
				STAGE_RAM[p][0].acc[i] += data[p][0].acc[i];
			STAGE_RAM[p][0].sum += data[p][0].sum;
		}

		msg.cmd = 'reply';
		msg.data = 'Shuffle done by worker ' + msg.id;
		msg.id = msg.from;		
		grid.send_cb(msg, function(err, res) {if (err) throw err;});

		if (nShuffle == worker.length) {
			nShuffle = 0;
			runStage(++stageIdx);
		}
	}
};

module.exports.buildTask = buildTask;
