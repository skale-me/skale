'use strict';

var fs = require('fs');
var Connection = require('ssh2');
var readline = require('readline');

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

// Rendre asynchrone buildTask
// Faire un premier treewalk à l'intérieur de build Task
// pour rechercher les noeuds de type hdfsTextFile
// et récupérer le breakdown des fichiers distants
// Le mapping worker block peut alors etre calculé
// et stocké dans la structure args associée au noeuds concernés
// à la fin de cette opération, on peut lance la compilation
// à l'issue de laquelle on appel le callback final rendant la main
// à la fonction appelant buildTask (située dans ugrid-array)
function preBuildTask(worker, array, action, callback) {
	var hdfsTextFileNodes = [],
		i;
	treewalk(array, null, function(a) {
		if (a.transform == 'hdfsTextFile')
			hdfsTextFileNodes.push(a);
	});
	// console.log(hdfsTextFileNodes[0].args[0]);
	// works only for one file in dag

	// If no hdfs query needed, return immediatly
	if (hdfsTextFileNodes.length == 0) {
		callback();
		return;
	}

	var hdfsTextFile = hdfsTextFileNodes[0].args[0];
	var conn = new Connection();
	var host = 'localhost';
	var username = 'cedric';
	var privateKey = '/home/cedric/.ssh/id_rsa';
	var bd = '/home/cedric/work/dist/hadoop-1.2.1'

	var fsck_cmd = bd + '/bin/hadoop fsck ' + hdfsTextFile + ' -files -blocks -locations';

	var regexp = /(\d+\. blk_-*\d+_\d+ len=\d+ repl=\d+ [[][0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+\])/i;
	var blocks = [];
	conn.on('ready', function() {
		conn.exec(fsck_cmd, function(err, stream) {
			if (err) throw err;

			var rl = readline.createInterface({
				input: stream.stdout,
				output: process.stdout,
				terminal: false
			});
			stream.stdout.setEncoding('utf8');
			stream.stderr.setEncoding('utf8');

			rl.on("line", function(line) {
				if (line.search(regexp) == -1)
					return;
				var v = line.split(' ');
				blocks.push({
					blockNum: parseFloat(v[0]),
					file: '/tmp/hadoop-cedric/dfs/data/current/' + v[1].substr(0, v[1].lastIndexOf('_')),
					host: v[4].substr(0, v[4].lastIndexOf(':')).replace('[', '')
				});
			});
			rl.on('close', function() {
				conn.end();
				// Boucler sur les blocks
				// pour chaque bloc chercher le worker
				if (worker.length > 1)
					throw 'hdfsTextFile only for one worker for now'
				var args = [
					[]
				];
				args[0][0] = blocks;
				// console.log(worker)
				// console.log(" ")
				hdfsTextFileNodes[0].args = args;
				// console.log(hdfsTextFileNodes[0].args[0][0])
				// Pour le moment on affect tous les blocks au meme worker
				callback();
			});
		})
	}).connect({
		host: host,
		username: username,
		privateKey: require('fs').readFileSync(privateKey)
	});
}

function buildTask(worker, array, action, callback) {
	preBuildTask(worker, array, action, function() {
		var nStage = 0,
			lastStage = 0,
			num = 0,
			code = '';
		var task = new Array(worker.length);
		for (var i = 0; i < task.length; i++)
			task[i] = {
				node: {}
			};
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
			stage[i] = {
				lineages: []
			};
		// ---------------------------------------------------------------------- //
		// Treewalk 2: Restore stage index, gen lineages code and set persistency
		// ---------------------------------------------------------------------- //
		treewalk(array, null, function(a) {
			var i, tmp, t1;
			// NB Move the argument mapper to ugrid-array.js
			for (i = 0; i < worker.length; i++) {
				if (a.transform == 'parallelize') {
					task[i].node[a.num] = {
						args: [a.args[0][i]]
					};
				} else if (a.transform == 'randomSVMData') {
					task[i].node[a.num] = {
						args: [a.args[0], a.args[1][i]]
					};
				} else if (a.transform == 'textFile') {
					task[i].node[a.num] = {
						args: [a.args[0], a.args[1], a.args[2][i]]
					};
				} else if (a.transform == 'hdfsTextFile') {
					task[i].node[a.num] = {
						args: [a.args[0][i]]
					}; // blocks affectés au worker i
				} else task[i].node[a.num] = {
					args: a.args
				};
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
				stage[a.stageIdx].lineages = [{
					pipeline: '',
					input: tmp
				}];
			} else if (a.child.length === 0) {
				// If we leave a leaf, add a new lineage to current stage
				if (a.transform == 'parallelize') {
					function parallelize(num, persistent, id) {
						var partition = node[num].args[0];
						if (persistent) {
							RAM[id] = {};
							for (var p in partition) RAM[id][p] = [];
						}
						for (var p in partition) {
							for (var i = 0; i < partition[p].length; i++) {
								tmp = partition[p][i];
								"PIPELINE_HERE"
							}
						}
					}
					stage[a.stageIdx].lineages.push({
						pipeline: a.persistent ? 'RAM[id][p].push(tmp);' : '',
						input: parallelize.toString() + ';' +
							'parallelize(' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ');'
					});
				} else if (a.transform == 'randomSVMData') {
					function randomSVMData(num, persistent, id) {
						var tmp, p, i, rng;
						var D = node[num].args[0];
						var partition = node[num].args[1];
						if (persistent) {
							RAM[id] = {};
							for (p in partition) RAM[id][p] = [];
						}
						for (p in partition) {
							rng = new ml.Random(partition[p].seed);
							for (i = 0; i < partition[p].n; i++) {
								tmp = ml.randomSVMLine(rng, D);
								"PIPELINE_HERE"
							}
						}
					}
					stage[a.stageIdx].lineages.push({
						pipeline: a.persistent ? 'RAM[id][p].push(tmp);' : '',
						input: randomSVMData.toString() + ';' +
							'randomSVMData(' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ');'
					});
				} else if (a.transform == 'textFile') {
					function textFile(num, persistent, id, stageIdx) { // attributes of node a
						var file = node[num].args[0];
						var P = node[num].args[1];
						var partitionIdx = node[num].args[2];
						var partitionLength = {};
						for (var p = 0; p < partitionIdx.length; p++)
							partitionLength[partitionIdx[p]] = 0;
						if (persistent) {
							RAM[id] = {};
							for (var p = 0; p < partitionIdx.length; p++)
								RAM[id][partitionIdx[p]] = [];
						}
						var l = 0;
						stage_locked[stageIdx] = true;
						var rl = readline.createInterface({
							input: fs.createReadStream(file),
							output: process.stdout,
							terminal: false
						});
						rl.on("line", function(tmp) {
							var p = l++ % P;
							if (partitionIdx.indexOf(p) != -1) {
								var i = partitionLength[p] ++;
								"PIPELINE_HERE"
							}
						});
						rl.on("close", function() {
							stage_locked[stageIdx] = false;
							if (stage_length[stageIdx] == ++stage_cnt[stageIdx])
								callback(res);
						});
					}
					stage[a.stageIdx].lineages.push({
						pipeline: a.persistent ? 'RAM[id][p].push(tmp);' : '',
						input: textFile.toString() + ';' +
							'textFile(' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ', ' + a.stageIdx + ');'
					});
				} else if (a.transform == 'hdfsTextFile') {
					function hdfsTextFile(blockIdx, num, persistent, id, stageIdx) {
						var file = node[num].args[0][blockIdx].file;
						var partitionIdx = [node[num].args[0][blockIdx].blockNum];
						var partitionLength = {};
						for (var p = 0; p < partitionIdx.length; p++)
							partitionLength[partitionIdx[p]] = 0;
						if (persistent) {
							RAM[id] = {};
							for (var p = 0; p < partitionIdx.length; p++)
								RAM[id][partitionIdx[p]] = [];
						}
						var l = 0;
						stage_locked[stageIdx] = true;
						var rl = readline.createInterface({
							input: fs.createReadStream(file),
							output: process.stdout,
							terminal: false
						});
						rl.on("line", function(tmp) {
							var i = partitionLength[partitionIdx[0]]++;
							"PIPELINE_HERE"
						});
						rl.on("close", function() {
							stage_locked[stageIdx] = false;
							if (++blockIdx < node[num].args[0].length) {
								console.log(blockIdx);
								hdfsTextFile(blockIdx, num, persistent, id, stageIdx);
							} else if (stage_length[stageIdx] == ++stage_cnt[stageIdx])
								callback(res);
						});
					}
					stage[a.stageIdx].lineages.push({
						pipeline: a.persistent ? 'RAM[id][p].push(tmp);' : '',
						input: hdfsTextFile.toString() + ';' +
							'hdfsTextFile(0, ' + a.num + ', ' + a.persistent.toString() + ', ' + a.id + ', ' + a.stageIdx + ');'
					});
				}
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
					stage[a.stageIdx].lineages = [{
						pipeline: tmp,
						input: t1
					}];
				}
				// Else append pipeline to current stage lineages
				else
					for (i = 0; i < stage[a.stageIdx].lineages.length; i++)
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

		fs.writeFileSync('/tmp/debug.js', taskCode);
		// throw 'break';

		for (i = 0; i < worker.length; i++) {
			task[i].action = action;
			task[i].task = taskCode;
		}

		callback(task);
	});
}

var taskTemplate = function(grid, fs, readline, ml, STAGE_RAM, RAM, node, action) {
	var stageIdx = 0,
		nShuffle = 0,
		finalCallback;
	var worker = [],
		stage = [],
		transform = {};

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
					} else
						shuffleRPC(worker[i], map[i], function(err) {
							if (err) throw err;
						});
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