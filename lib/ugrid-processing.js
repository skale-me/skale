'use strict';

var ml = require('./ugrid-ml.js');
var Source = require('./ugrid-sources.js');
var RDD = require('./ugrid-transformation.js');

/*
	introduire grid, app, job, node comme interface de transform
*/

module.exports.UgridJob = function(grid, app, param) {
	var node = param.node;
	var stageData = param.stageData;
	var actionData = param.actionData;
	var jobId = param.jobId;
	var stageIdx = 0, stage = [], nStage = stageData.length;

	// Instantiate action and output stream
	var action = new RDD[actionData.fun](jobId, grid, actionData);
	// var action = new RDD[actionData.fun](grid, app, job, actionData);
	var stream = grid.createWriteStream(jobId, app.master_uuid);
	var job = {id: jobId, node: node, action: action};

	// Instantiate transformations (and sources ASAP)
	for (var i in node) {
		if (node[i].transform == 'stream')
			app.completedStreams[node[i].args[1]] = false;
		if (RDD[node[i].transform])
			node[i].transform = new RDD[node[i].transform](jobId, grid, node[i], app.worker);
			// node[i].transform = new RDD[node[i].transform](grid, app, job, node[i]);
	}

	// instantiate stages
	for (i = 0; i < nStage; i++)
		stage[i] = new Stage(grid, app, job, i, stageData[i], nextStage, i == (nStage - 1));

	function nextStage(sid, ignore) {
		if (sid != stageIdx) return;
		// si dernier stage on clos l'action et on prépare l'itération d'après
		if (sid == (nStage - 1)) {
			if ((app.wid === 0) || (++action.semaphore == 2)) {
				if (!ignore) {
					stream.write(action.result);
					// action.sendResult();
					action.reset();
					action.semaphore = 0;
					if (app.worker[app.wid + 1])
						grid.send(app.worker[app.wid + 1].uuid, {cmd: 'action', jobId: jobId});
				}
				for (var i in app.dones) app.dones[i]();
				app.dones = {};
				var jobFinished = true;
				for (var s in app.completedStreams) {
					if (!app.completedStreams[s]) {
						jobFinished = false;
						break;
					}
				}
				if (jobFinished)
					grid.send(app.master_uuid, {cmd: 'endJob', data: jobId});
				stage[sid].target_cnt = stage[sid].next_target_cnt;
				stage[sid].cnt = 0;
			}
		} else if (stage[stageIdx].state.nShuffle == app.worker.length)
			stage[++stageIdx].run();
	}

	this.run = function() {stage[0].run();};

	this.processShuffle = function(msg) {
		stage[stageIdx].processShuffle(msg, nextStage);
	};

	this.processAction = function(msg) {
		if (action.semaphore == 1) nextStage(stage.length - 1);
		else ++action.semaphore;
	};

	this.processLastLine = function(msg) {
		stage[stageIdx].processLastLine(msg);
	};
};

function Stage(grid, app, job, stageIdx, stageData, nextStage, isLastStage) {
	var node = job.node;
	var lineages = stageData.lineages;
	var shuffleNum = stageData.shuffleNum;
	var state = this.state = {
		cnt: 0,								// Number of finished lineages
		target_cnt: lineages.length,		// Number of lineages
		locked: false,						// Because of an asynchonous lineage
		nShuffle: 0,						// Number of shuffle received
		sid: stageIdx,						// Indice du stage
		next_target_cnt: lineages.length	// next iteration lineage target count
	};
	var source = [];

	for (var l = 0; l < lineages.length; l++)
		source[l] = new Source[node[lineages[l][0]].type](grid, app, job, l, state, lineages[l], isLastStage);

	this.run = function () {
		for (var l = 0; l < source.length; l++) {
			try {
				source[l].run(function(streamIdx, done, ignore) {
					if (done) app.dones[streamIdx] = done;
					if ((++state.cnt < state.target_cnt) || state.locked) return;
					if (!isLastStage) node[shuffleNum].transform.tx_shuffle(state);
					nextStage(state.sid, ignore);
				});
			} catch (err) {
				console.error(err.stack);
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

	this.processLastLine = function (msg) {
		source[msg.args.lid].processLastLine(msg.args);
	};
}
