'use strict';

var ml = require('./ugrid-ml.js');
var Source = require('./ugrid-sources.js');
var RDD = require('./ugrid-transformation.js');
var trace = require('line-trace');

module.exports.UgridJob = function(grid, app, param) {
	var stageIdx = 0, stage = [], nStage = param.stageData.length;
	var job = {id: param.jobId, node: param.node};
	var action = job.action = new RDD[param.actionData.fun](grid, app, job, param.actionData);

	for (var i = 0; i < nStage; i++) {
		var stageParam = {sid: i, stageData: param.stageData[i], isLastStage: i == (nStage - 1)};
		stage[i] = new Stage(grid, app, job, stageParam, nextStage);
	}

	function nextStage(sid, ignore) {
		if (sid != stageIdx) return;
		if (sid == (nStage - 1)) {
			if ((app.wid === 0) || (++action.semaphore == 2)) {
				if (!ignore) {
					action.sendResult();
					// reset action and nodes
					action.reset();
					for (var n in job.node) 
						if (n.reset) n.reset();
					// set next iteration starting stage
					for (var i = 0; i < stage.length; i++)
						if (stage[i].state.next_target_cnt != 0)
							break;
					if (i < stage.length) {
						stageIdx = i;
					} else console.log('Job is finished');
				}
				// unlock distant streams
				for (var i in app.dones) app.dones[i]();
				app.dones = {};
				// Stream is finished not at a block frontier
				// job is finished if all stages are finished
				var jobFinished = true;
				for (var s in app.completedStreams) {
					if (!app.completedStreams[s]) {
						jobFinished = false;
						break;
					}
				}
				if (jobFinished)
					grid.send(app.master_uuid, {cmd: 'endJob', data: param.jobId});
				stage[sid].target_cnt = stage[sid].next_target_cnt;
				stage[sid].cnt = 0;
			}
		} else if (ignore == true) {
			// We got endJob at block frontier
			// unlock distant streams
			for (var i in app.dones) app.dones[i]();
			app.dones = {};
			// job is finished if all stages are finished
			var jobFinished = true;
			for (var s in app.completedStreams) {
				if (!app.completedStreams[s]) {
					jobFinished = false;
					break;
				}
			}
			if (jobFinished)
				grid.send(app.master_uuid, {cmd: 'endJob', data: param.jobId});
		} else if (stage[stageIdx].state.nShuffle == app.worker.length)
			stage[++stageIdx].run();
	}

	this.run = function() {stage[0].run();};

	this.processShuffle = function(msg) {
		stage[msg.sid].processShuffle(msg, nextStage);
	};

	this.processAction = function(msg) {
		if (action.semaphore == 1) {
			nextStage(stage.length - 1);
		}
		else ++action.semaphore;
	};

	this.processLastLine = function(msg) {
		stage[msg.args.sid].processLastLine(msg);
	};
};

function Stage(grid, app, job, param, nextStage) {
	var node = job.node;
	var lineages = param.stageData.lineages;
	var shuffleNum = param.stageData.shuffleNum;
	var source = [];	
	var state = this.state = {
		cnt: 0,								// Number of finished lineages
		target_cnt: lineages.length,		// Number of lineages
		locked: false,						// Because of an asynchonous lineage
		nShuffle: 0,						// Number of shuffle received
		sid: param.sid,						// Indice du stage
		next_target_cnt: lineages.length	// next iteration lineage target count
	};

	for (var l = 0; l < lineages.length; l++) {
		for (var j = 1; j < lineages[l].length; j++) {
			var n = node[lineages[l][j]];
			if (typeof(n.transform) == 'string')
				n.transform = new RDD[n.transform](grid, app, job, state, n);
		}
		var sourceParam = {lid: l, transform: lineages[l], inLastStage: param.isLastStage};
		source[l] = new Source[node[lineages[l][0]].type](grid, app, job, state, sourceParam);
	}

	this.run = function () {
		for (var l = 0; l < source.length; l++) {
			try {
				source[l].run(function(streamIdx, done, ignore) {
					if (done) app.dones[streamIdx] = done;
					if ((++state.cnt < state.target_cnt) || state.locked) return;
					if (!param.isLastStage) node[shuffleNum].transform.tx_shuffle();
					nextStage(state.sid, ignore);
				});
			} catch (err) {
				console.error(err.stack);
				throw new Error("Lineage error, " + err);
			}
		}
	};

	this.processShuffle = function (msg) {
		try {node[shuffleNum].transform.rx_shuffle(msg.args);} 
		catch (err) {throw new Error("Lineage rx shuffle " + node[shuffleNum].type + ": " + err);}
		nextStage(state.sid);
	};

	this.processLastLine = function (msg) {
		source[msg.args.lid].processLastLine(msg.args);
	};
}
