'use strict';

var util = require('util');
var thunkify = require('thunkify');
var url = require('url');

var UgridClient = require('./ugrid-client.js');
var UgridArray = require('./ugrid-array.js');

module.exports = UgridContext;

util.inherits(UgridContext, UgridClient);

function UgridContext(arg) {
	if (!(this instanceof UgridContext))
		return new UgridContext(arg);

	var self = this, worker = this.worker = [];
	arg = arg || {};
	arg.data = arg.data || {type: 'master'};
	var grid = this.grid = new UgridClient(arg);

	this.init_cb = function (callback) {
		grid.devices_cb({type: 'worker'}, function(err, res) {
			for (var i = 0; i < res.length; i++)
				self.worker[i] = new Worker(res[i]);
			callback();
		});
	};

	function Worker(w) {
		this.uuid = w.uuid;
		this.id = w.id;
		this.ip = w.ip;
	}

	Worker.prototype.rpc = function (cmd, args, callback) {
		grid.request_cb({uuid: this.uuid, id: this.id}, {cmd: cmd, args: args}, callback);
	};

	this.end = function () {
		grid.end();
	};

	this.randomSVMData = function (N, D, seed, nPartitions) {
		// by default number of partitions equals number of workers
		var i, p, partitions = [], data = [], acc, wid = 0;
		var P = nPartitions || worker.length;

		// Create partitions array
		for (p = 0; p < P; p++) partitions.push({n: 0});
		// Set partitions length
		p = 0;
		for (i = 0; i < N; i++) {
			partitions[p].n++;
			p = (p + 1) % P;
		}
		// Set partitions seed
		acc = seed;
		for (p = 0; p < P; p++) {
			partitions[p].seed = acc;
			acc += partitions[p].n * (D + 1);
		}

		var a = new UgridArray(grid, worker, [], 'narrow', 'randomSVMData', [data]);
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = [];
			data[wid].push(partitions[p]);
			wid = (wid + 1) % worker.length;
		}
		a.args = [data];
		a.getArgs = function(wid) {
			return {args: [D, a.args[0][wid]]};
		}
		return a;
	};

	this.parallelize = function (localArray, nPartitions) {
		var a = new UgridArray(grid, worker, [], 'narrow', 'parallelize', []);
		a.getArgs = function(w) {
			var data = [], wid = 0;
			var P = nPartitions || worker.length;

			function split(a, n) {
				var len = a.length, out = [], i = 0;
				while (i < len) {
					var size = Math.ceil((len - i) / n--);
					out.push(a.slice(i, i += size))
				}
				return out;
			}
			var partitions = split(localArray, P);

			// Map partitions to workers
			for (var p = 0; p < P; p++) {
				if (data[wid] === undefined) data[wid] = [];
				data[wid].push(partitions[p]);
				wid = (wid + 1) % worker.length;
			}
			return {args: [data[w]]};
		}
		return a;
	};

	this.textFile = function (path, nPartitions) {
		var u = url.parse(path);
		// hdfs file
		if ((u.protocol == 'hdfs:') && u.slashes && u.hostname && u.port) {
			var a = new UgridArray(grid, worker, [], 'narrow', 'hdfsTextFile', [u.path]);
			a.getArgs = function(wid) {
				// here args has been modified during prebuildTask
				// hence it can't be deducted yet from path
				return {args: [a.args[0][wid]]};
			}
			return a;
		}

		// local file
		// by default number of partitions equals number of workers
		var i, p, data = [], wid = 0;
		var P = nPartitions || worker.length;

		var a = new UgridArray(grid, worker, [], 'narrow', 'textFile', [data]);
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = [];
			data[wid].push(a.id + '.' + p);
			wid = (wid + 1) % worker.length;
		}
		// Padd data with zero length arrays if needed
		if (P < worker.length)
			for (i = P; i < worker.length; i++)
				data[i] = [];

		a.args = [data];

		a.getArgs = function(wid) {
			return {args: [path, P, a.args[0][wid]]};
		}
		return a;
	}

	this.init = thunkify(this.init_cb);
}
