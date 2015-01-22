'use strict';

var util = require('util');
var thunkify = require('thunkify');
var UgridClient = require('./ugrid-client.js');
var UgridArray = require('./ugrid-array.js');

module.exports = UgridContext;

util.inherits(UgridContext, UgridClient);

function UgridContext(arg) {
	if (!(this instanceof UgridContext))
		return new UgridContext(arg);

	var grid = this.grid = new UgridClient({
		host: (arg && arg.host) || process.env.UGRID_HOST || 'localhost',
		port: (arg && arg.port) || process.env.UGRID_PORT || 12346,
		data: (arg && arg.data) || {type: 'master'}
	});

	var self = this, worker = this.worker = [];

	this.init_cb = function (callback) {
		grid.connect_cb(function () {
			grid.devices_cb({type: 'worker'}, function(err, res) {
				for (var i = 0; i < res.length; i++)
					self.worker[i] = new Worker(res[i]);
				callback();
			});
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
		grid.disconnect();
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
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = {};
			data[wid][p] = partitions[p];
			wid = (wid + 1) % worker.length;
		}

		return new UgridArray(grid, worker, [], 'narrow', 'randomSVMData', [D, data]);
	};

	this.parallelize = function (localArray, nPartitions) {
		// by default number of partitions equals number of workers
		var i, p, partitions = [], data = [], wid = 0;
		var P = nPartitions || worker.length;

		// Create partitions array
		for (p = 0; p < P; p++) partitions.push([]);
		// Fill partitions with localArray data
		p = 0;
		for (i = 0; i < localArray.length; i++) {
			partitions[p].push(localArray[i]);
			p = (p + 1) % P;
		}
		// Map partitions to workers
		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = {};
			data[wid][p] = partitions[p];
			wid = (wid + 1) % worker.length;
		}

		return new UgridArray(grid, worker, [], 'narrow', 'parallelize', [data]);
	};

	this.textFile = function (path, nPartitions) {
		// by default number of partitions equals number of workers
		var i, p, data = [], wid = 0;
		var P = nPartitions || worker.length;

		for (p = 0; p < P; p++) {
			if (data[wid] === undefined) data[wid] = [];
			data[wid].push(p);
			wid = (wid + 1) % worker.length;
		}
		// Padd data with zero length arrays if needed
		if (P < worker.length)
			for (i = P; i < worker.length; i++)
				data[i] = [];

		return new UgridArray(grid, worker, [], 'narrow', 'textFile', [path, P, data]);
	};

	this.hdfsTextFile = function (path) {
		return new UgridArray(grid, worker, [], 'narrow', 'hdfsTextFile', [path]);
	};

	this.init = thunkify(this.init_cb);
}
