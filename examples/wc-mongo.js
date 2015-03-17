#!/usr/local/bin/node

// Word count, stream mode

'use strict';

var assert = require('assert');
var grid = require('../lib/ugrid-context.js')();
var MongoClient = require('mongodb').MongoClient;

var file = process.argv[2] || '/etc/hosts';

// Worker task constructor
function WorkerTask(grid, fs, Lines, ml, STAGE_RAM, RAM, msg) {
	var file = msg.data.args.file;
	var rank = msg.data.args.rank;
	var wmax = msg.data.args.wmax;
	var count = 0;
	var master = {uuid: msg.ufrom, id: msg.from};
	var lines = new Lines();

	// fs.createReadStream(file).pipe(lines);

	//this.run = function (callback) {
	//	var msg = {id: master.id, cmd: 'line'};
	//	grid.setInputStream(lines);
	//	lines.on('data', function (line) {
	//		if (wmax > 1 && (count++ % wmax != rank)) return;
	//		var w, words = line.split(/\W+/), res = {};
	//		for (var i in words) {
	//			w = words[i];
	//			if (!w) continue;
	//			res[w] = res[w] ? res[w] + 1: 1;
	//		}
	//		msg.data = res;
	//		grid.send(0, msg);
	//	});
	//	lines.on('end', function () {
	//		console.log('sending end to master');
	//		msg.cmd = 'end';
	//		msg.data = '';
	//		grid.send(0, msg);
	//	});
	//};

	console.log("master id: " + msg.from);
	this.run = function (callback) {
		var wcStream = grid.createWriteStream(master, 'line');
		fs.createReadStream(file).pipe(lines).pipe(wcStream);
	};

	function wordcount(line) {
		if (wmax > 1 && (count++ % wmax != rank)) return;
		var w, words = line.split(/\W+/), res = {};
		for (var i in words) {
			w = words[i];
			if (!w) continue;
			res[w] = res[w] ? res[w] + 1: 1;
		}
		return res;
	}
}

var words = {}, finished = 0;
//var pending = 0, remotePaused = [];
var pending = 0, remotePaused = false;

MongoClient.connect('mongodb://localhost:27017/test', function (err, db) {
	assert.equal(null, err);
	grid.init(function () {
		var task = {
			task: WorkerTask.toString(),
			file: file,
			wmax: grid.worker.length
		};
		var dwords = db.collection('words');
		for (var i = 0; i < grid.worker.length; i++) {
			task.rank = i;
			grid.worker[i].rpc('setTask',  task);
			grid.worker[i].rpc('runTask');
		}
		grid.grid.on('line', function (msg) {
			console.log('line: ' + msg);
			for (var i in msg.data) {
				//if (!words[i])
				//	words[i] = msg.data[i];
				//else
				//	words[i] += msg.data[i];
				pending++;
				//if (pending > 100 && !remotePaused[msg.from]) {
				if (pending > 100 && !remotePaused) {
					console.log('pause ' + msg.from);
					//remotePaused[msg.from] = true;
					console.log('pause');
					remotePaused = true;
					//grid.grid.send(1, {cmd: 'pause'});
				}
				dwords.update({name: i}, {$inc: {count: msg.data[i]}}, {w:0, upsert: true, safe: false}, function () {
					pending--;
					if (pending < 10) {
						if (remotePaused) {
							console.log('resume');
							// grid.grid.send(1, {cmd: 'resume'});
							remotePaused = false;
						}
						//for (var i in remotePaused) {
						//	if (!remotePaused[i]) continue;
						//	console.log('resume ' + i);
						//	remotePaused[i] = false;
						//	grid.grid.send(0, {cmd: 'resume', id: i});
						//}
					}
					console.log('pending: ' + pending);
				});
			}
		});
		grid.on('end', function (msg) {
			console.log('got end from worker');
			if (++finished < grid.worker.length) return;
			//console.log(words);
			db.close();
			process.exit(0);
		});
	});
});
