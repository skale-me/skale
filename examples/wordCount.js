#!/usr/local/bin/node --harmony

// Word count, stream mode

'use strict';

var co = require('co');
var grid = require('../lib/ugrid-context.js')();

var file = process.argv[2] || '/etc/hosts';

// Worker task constructor
function WorkerTask(grid, fs, readline, ml, STAGE_RAM, RAM, msg) {
	var file = msg.data.args.file;
	var rank = msg.data.args.rank;
	var wmax = msg.data.args.wmax;
	var master = {uuid: msg.ufrom, id: msg.from};
	var rl = readline.createInterface({
		input: fs.createReadStream(file, {encoding: 'utf8'}),
		output: process.stdout, terminal: false
	});

	this.run = function (callback) {
		var msg = {id: master.id, cmd: 'line'};
		var count = 0;
		rl.on('line', function (line) {
			if (wmax > 1 && (count++ % wmax != rank)) return;
			var w, words = line.split(/\W+/), res = {};
			for (var i in words) {
				w = words[i];
				if (!w) continue;
				res[w] = res[w] ? res[w] + 1: 1;
			}
			msg.data = res;
			grid.send_cb(0, msg);
		});
		rl.on('close', function () {
			msg.cmd = 'end';
			msg.data = '';
			grid.send_cb(0, msg);
		});
	};
}

co(function *() {
	var words = {}, finished = 0;
	yield grid.init();
	for (var i = 0; i < grid.worker.length; i++) {
		grid.worker[i].rpc('setTask',  {task: WorkerTask.toString(), file: file, rank: i, wmax: grid.worker.length});
		grid.worker[i].rpc('runTask');
	}
	grid.on('line', function (msg) {
		for (var i in msg.data) {
			if (!words[i])
				words[i] = msg.data[i];
			else
				words[i] += msg.data[i];
		}
	});
	grid.on('end', function (msg) {
		if (++finished < grid.worker.length) return;
		console.log(words);
		process.exit(0);
	});
})();
