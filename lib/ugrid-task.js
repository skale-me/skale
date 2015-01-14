'use strict';

var fs = require('fs');

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
	var nStage = 0, lastStage = 0, num = 0, code = '';
	var task = new Array(worker.length);
	for (var i = 0; i < task.length; i++)
		task[i] = {node: {}};
	// ---------------------------------------------------------------------- //
	// Treewalk 1: Set stage index, node index and stage array
	// ---------------------------------------------------------------------- //
	treewalk(array, function (a) {
		if (a.transformType == 'wide') nStage++;
		if (lastStage < nStage) lastStage = nStage;
	}, function (a) {
		a.stageIdx = nStage;
		a.num = num++;
		if (a.transformType == 'wide') {
			nStage--;
		}
	});
	var stage = new Array(lastStage + 1);
	for (i = 0; i < stage.length; i++)
		stage[i] = {lineages: []};
	// ---------------------------------------------------------------------- //
	// Treewalk 2: Restore stage index, gen lineages code and set persistency
	// ---------------------------------------------------------------------- //
	treewalk(array, null, function (a) {
		var i, tmp, t1;
		// NB Move the argument mapper to ugrid-array.js
		for (i = 0; i < worker.length; i++) {
			if (a.transform == 'parallelize') {
				task[i].node[a.num] = {args: [a.args[0][i]]};
			} else if (a.transform == 'randomSVMData') {
				task[i].node[a.num] = {args: [a.args[0], a.args[1][i]]};
			} else if (a.transform == 'textFile') {
				task[i].node[a.num] = {args: [a.args[0], a.args[1], a.args[2][i]]};
			} else task[i].node[a.num] = {args: a.args};
		}
		// reverse stage index
		a.stageIdx = lastStage - a.stageIdx;
		// If we leave an inMemory array, reset current stage to start here,
		// TODO: discard previous stages
		if (a.inMemory) {
			tmp = (a.stageIdx && lastStage) ?
				'var input = STAGE_RAM;\n' : 'var input = RAM[' + a.id + '];\n';
			tmp += 'for (var p in input) {' +
						'for (var i = 0; i < input[p].length; i++) {' +
							'var tmp = input[p][i];' +
							'"PIPELINE_HERE"' +
						'}}';
			stage[a.stageIdx].lineages = [{pipeline: '', input: tmp}];
		} else if (a.child.length === 0) {
			// If we leave a leaf, add a new lineage to current stage
			// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX //
			t1 = '';
			if (a.transform == 'parallelize') {
				t1 += 'var partition = node[' + a.num + '].args[0];';
				if (a.persistent) {
					t1 += 'RAM[' + a.id + '] = {};';
					t1 += 'for (var p in partition) RAM[' + a.id + '][p] = [];';
				}
				t1 += 'for (var p in partition) {';
				t1 += 'for (var i = 0; i < partition[p].length; i++) {';
				t1 += 'tmp = partition[p][i];';
				if (a.persistent)
					t1 += 'RAM[' + a.id + '][p].push(tmp);';
				t1 += '"PIPELINE_HERE"}}';
			// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX //
			} else if (a.transform == 'randomSVMData') {
				t1 += 'var D = node[' + a.num + '].args[0];';
				t1 += 'var partition = node[' + a.num + '].args[1];';
				if (a.persistent) {
					t1 += 'RAM[' + a.id + '] = {};';
					t1 += 'for (var p in partition) RAM[' + a.id + '][p] = [];';
				}
				t1 += 'for (var p in partition) {';
				t1 += 'var rng = new ml.Random(partition[p].seed);';
				t1 += 'for (var i = 0; i < partition[p].n; i++) {';
				t1 += 'tmp = ml.randomSVMLine(rng, D);';
				if (a.persistent)
					t1 += 'RAM[' + a.id + '][p].push(tmp);';
				t1 += '"PIPELINE_HERE"}}';
			// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX //
			} else if (a.transform == 'textFile') {
				// t1 += 'console.log(node[' + a.num + ']);'; // DEBUG

				t1 += 'var file = node[' + a.num + '].args[0];';
				t1 += 'var P = node[' + a.num + '].args[1];';
				t1 += 'var partitionIdx = node[' + a.num + '].args[2];';
				t1 += 'var partitionLength = {}; for (var p = 0; p < partitionIdx.length; p++) partitionLength[partitionIdx[p]] = 0;';
				if (a.persistent)
					t1 += 'RAM[' + a.id + '] = {};' +
						'for (var p = 0; p < partitionIdx.length; p++) RAM[' + a.id + '][partitionIdx[p]] = [];';
				t1 += 'var l = 0;';
				t1 += 'stage_locked[' + a.stageIdx + '] = true;';
				t1 += 'var rl = readline.createInterface({input: fs.createReadStream(file), output: process.stdout, terminal: false});';
				t1 += 'rl.on("line", function (tmp) {';
				t1 += 'var p = l++ % P;';
				
				// t1 += 'console.log(p);';
				// t1 += 'console.log(partitionIdx.indexOf(p));';

				t1 += 'if (partitionIdx.indexOf(p) != -1) {';
				t1 += 'var i = partitionLength[p]++;';
				if (a.persistent)
					t1 += 'RAM[' + a.id + '][p].push(tmp);';
				t1 += '"PIPELINE_HERE"';
				t1 += '}});';
				t1 += 'rl.on("close", function () {';
				t1 += 'stage_locked[' + a.stageIdx + '] = false;';
				t1 += 'if (stage_length[' + a.stageIdx + '] == ++stage_cnt[' + a.stageIdx + '])';
				t1 += 'callback(res);';
				t1 += '});';
			}
			// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX //
			stage[a.stageIdx].lineages.push({pipeline: '', input: t1});
		} else {
			tmp = a.pipelineSource();
			code += a.transformSource();
			if (a.persistent && (a.transformType == 'narrow')) {
				// Init RAM if needed
				tmp += 'if (RAM[' + a.id + '] == undefined) RAM[' + a.id + '] = {};\n';
				tmp += 'if (RAM[' + a.id + '][p] == undefined) RAM[' + a.id + '][p] = [];\n';
				tmp += '\t\tRAM[' + a.id + '][p].push(tmp);\n';
			}
			// If it's the begining of a new stage, create a new lineage
			if (stage[a.stageIdx].lineages.length === 0) {
				t1 = 'var input = STAGE_RAM; for (var p in input) {' +
					'for (var i = 0; i < input[p].length; i++) {var tmp = input[p][i];' +
					'"PIPELINE_HERE"}}';
				stage[a.stageIdx].lineages = [{pipeline: tmp, input: t1}];
			}
			// Else append pipeline to current stage lineages
			else for (i = 0; i < stage[a.stageIdx].lineages.length; i++)
					stage[a.stageIdx].lineages[i].pipeline += tmp;
		}
		// Set persistency for next build
		a.inMemory = a.persistent;
	});
	// ---------------------------------------------------------------------- //
	// Step 3: Write source code of Task() constructor
	// ---------------------------------------------------------------------- //
	for (i = 0; i < worker.length; i++)
		code += 'worker[' + i + '] = ' + JSON.stringify(worker[i]) + ';\n';
	code += 'var stage_length = [];';
	code += 'var stage_cnt = [];';
	code += 'var stage_locked = [];';
	for (i = 0; i < stage.length; i++) {
		code += 'stage_length[' + i + '] = ' + stage[i].lineages.length + ';';
		code += 'stage_cnt[' + i + '] = 0;';
		code += 'stage_locked[' + i + '] = false;';
		code += 'stage[' + i + '] = function(callback) {\n';
		code += (i == lastStage) ? action.init : 'var res = {};\n';
		for (var j = 0; j < stage[i].lineages.length; j++) {
			var tmp = stage[i].lineages[j].pipeline;
			if (i == lastStage)
				tmp += action.run;
			code += stage[i].lineages[j].input.replace('"PIPELINE_HERE"', tmp);
		}
		code += action.post || '';
		code += 'if (!stage_locked[' + i + '] && (stage_length[' + i + '] == ++stage_cnt[' + i + ']))';
		code += 'callback(res);';
		code += '}\n';
	}

	var taskCode = taskTemplate.toString().replace('"WORKERS_AND_STAGES"', code);

	// fs.writeFile('/tmp/debug.js', taskCode);

	for (i = 0; i < worker.length; i++) {
		task[i].action = action;
		task[i].task = taskCode;
	}

	return task;
}

var taskTemplate = function(grid, fs, readline, ml, STAGE_RAM, RAM, node, action) {
	var stageIdx = 0, nShuffle = 0, finalCallback;
	var worker = [], stage = [], transform = {};

	"WORKERS_AND_STAGES"

	function shuffleRPC(host, args, callback) {
		grid.request_cb(host, {cmd: 'shuffle', args: args}, callback);
	}

	function runStage(idx) {
		stage[idx](function (res) {
			if (stageIdx == (stage.length - 1)) {
				STAGE_RAM = [];
				finalCallback(res);
			} else {
				// Map partitions to workers
				var map = worker.map(function() {return {};});
				for (var p in res)
					map[p % worker.length][p] = res[p]; // Ok if partition name is a Number for now, use hashcoding later

				for (var i = 0; i < map.length; i++) {
					if (grid.host.uuid == worker[i].uuid) {
						nShuffle++;
						STAGE_RAM = map[i];
					} else
						shuffleRPC(worker[i], map[i], function (err) {if (err) throw err;});
				}
				// Run next stage if needed
				if (worker.length == 1) {
					nShuffle = 0;
					runStage(++stageIdx);
				}
			}
		});
	}

	this.run = function (callback) {
		finalCallback = callback;
		runStage(0);
	};

	this.processShuffle = function (msg) {
		nShuffle++;
		var data = msg.data.args;
		// Reduce intermediates results (to be generated programmatically)
		for (var p in data) {
			if (!STAGE_RAM[p] || !data[p]) continue;
			for (var i = 0; i < STAGE_RAM[p][0].acc.length; i++)
				STAGE_RAM[p][0].acc[i] += data[p][0].acc[i];
			STAGE_RAM[p][0].sum += data[p][0].sum;
		}

		grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);

		if (nShuffle == worker.length) {
			nShuffle = 0;
			runStage(++stageIdx);
		}
	};
};

module.exports.buildTask = buildTask;
