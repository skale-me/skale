'use strict';

var ml = require('./ugrid-ml.js');
var sources = require('./ugrid-sources.js');
var rdd = require('./ugrid-transformation.js');

/*
	worker context:
		- grid
	application context:
		- worker
		- master_uuid
		- streams
		- RAM
		- dones
	job context:
		- jobId
		- node
	stage context:

*/

module.exports.UgridJob = function(request, jobId, grid, RAM, msg) {
	var node = msg.data.args.node;
	var worker = msg.data.args.worker;
	var stageData = msg.data.args.stageData;
	var action = stageData[stageData.length - 1].action;
	var stageIdx = 0, stage = [];
	var master_uuid = msg.data.master_uuid;
	var dones = {};
	var completedStreams = {};

	function recompile(s) {
		var args = s.match(/\(([^)]*)/)[1];
		var body = s.replace(/^function *[^)]*\) *{/, '').replace(/}$/, '');
		return new Function(args, body);
	}

	for (var i in node) {
		if (node[i].src) node[i].src = recompile(node[i].src);
		if (rdd[node[i].transform])
			node[i].transform = new rdd[node[i].transform](jobId, grid, node[i], worker);
		if (node[i].transform == 'stream')
			completedStreams[node[i].args[1]] = false;
	}

	if (action.src) {
		action.ml = ml;
		action.src = recompile(action.src);
	}

	for (i = 0; i < stageData.length; i++)
		stage[i] = new Stage(jobId, completedStreams, worker, grid, node, RAM, i, stageData[i], nextStage, master_uuid, dones);

	function nextStage(sid) {
		if (sid != stageIdx) return;
		if (stage[stageIdx].state.nShuffle == worker.length)
			stage[++stageIdx].run();
	}

	this.run = function() {stage[0].run();};

	this.processShuffle = function(msg) {
		// ici on appelle le processShuffle du stage en cours, mais
		// il est possible que le shuffle concerne un stage pas encore atteint
		// par le worker, auquel cas il est nécessaire de pouvoir
		// identifier au sein de msg l'id du stage concerné afin de pouvoir
		// ecrire
		// stage[msg.data.stageIdx].processShuffle(msg, nextStage);
		// aussi lorsque l'on appelle nextStage il faut passer
		// en argument le stage concerné afin de ne pas faire progresser
		// le stage en cours si ce n'est pas le même
		stage[stageIdx].processShuffle(msg, nextStage);
	};

	this.processAction = function(msg) {
		stage[stage.length - 1].processAction(msg);
	};

	this.processLastLine = function(msg) {
		stage[stageIdx].processLastLine(msg);
	};
};

function Stage(jobId, completedStreams, worker, grid, node, RAM, stageIdx, stageData, nextStage, master_uuid, dones) {
	var lineages = stageData.lineages; 		// Lineages vector
	var state = this.state = {
		cnt: 0,								// Number of finished lineages
		target_cnt: lineages.length,		// Number of lineages
		locked: false,						// Because of an asynchonous lineage
		nShuffle: 0,						// Number of shuffle received
		sid: stageIdx,						// Indice du stage
		next_target_cnt: lineages.length	// next iteration lineage target count
	};
	var shuffleNum = stageData.shuffleNum;
	var action = stageData.action ? new rdd[stageData.action.fun](jobId, grid, stageData.action) : null;
	var lineage = [];

	if (action) {
		action.semaphore = 0;
		var stream = grid.createWriteStream(jobId, master_uuid);
	}

	for (var wid = 0; wid < worker.length; wid++)
		if (worker[wid].uuid == grid.host.uuid) break;

	for (var l = 0; l < lineages.length; l++)
		lineage[l] = new sources[node[lineages[l][0]].type](jobId, completedStreams, l, grid, worker, state, node, RAM, lineages[l], action);

	this.run = function () {
		for (var l = 0; l < lineage.length; l++) {
			try {
				lineage[l].run(function(streamIdx, done, ignore) {
					console.log('in run callback, ignore: ' + ignore);
					if (done)
						dones[streamIdx] = done;
					if ((++state.cnt < state.target_cnt) || state.locked) return;
					if (action) {
						if ((wid === 0) || (++action.semaphore == 2)) {
							if (!ignore) {
								stream.write(action.result);
								action.reset();
								action.semaphore = 0;
								if (worker[wid + 1])
									grid.send(worker[wid + 1].uuid, {cmd: 'action', jobId: jobId});
							}
							for (var i in dones) dones[i]();
							dones = {};
						}
						var jobFinished = true;
						for (var s in completedStreams) {
							if (!completedStreams[s]) {
								jobFinished = false;
								break;
							}
						}
						if (jobFinished)
							grid.send(master_uuid, {cmd: 'endJob', data: jobId});
						// reinitialize barrier for next iteration, in case of streams
						state.target_cnt = state.next_target_cnt;
						state.cnt = 0;
					} else {
						try {
							node[shuffleNum].transform.tx_shuffle(state);
							nextStage(state.sid);
						} catch (err) {
							console.error(err.stack);
							throw new Error("Lineage tx shuffle " + node[shuffleNum].type + ": " + err);
						}
					}
				});
			} catch (err) {
				throw new Error("Lineage error, " + err);
			}
		}
	};

	this.processShuffle = function (msg, nextStage) {
		try {
			node[shuffleNum].transform.rx_shuffle(msg.data.args, state);
			grid.reply(msg, null, 'Shuffle done by worker ' + msg.id);
			nextStage(state.sid);
		} catch (err) {
			throw new Error("Lineage rx shuffle " + node[shuffleNum].type + ": " + err);
		}
	};

	this.processAction = function () {
		if (++action.semaphore == 2) {
			stream.write(action.result);
			action.reset();
			action.semaphore = 0;
			if (worker[wid + 1])
				grid.send(worker[wid + 1].uuid, {cmd: 'action', jobId: jobId});
			for (var i in dones) dones[i](); 		// unlock streams on master side
			dones = {};
		}
	};

	this.processLastLine = function (msg) {
		lineage[msg.data.args.lid].processLastLine(msg.data.args);
	};
}
