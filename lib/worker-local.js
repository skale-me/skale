// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

// worker module

'use strict';

var fs = require('fs');
var mkdirp = require('mkdirp');
var uuid = require('node-uuid');
var sizeOf = require('./rough-sizeof.js');
var Lines = require('./lines.js');
var readSplit = require('./readsplit.js').readSplit;

var memory = process.argv[3] || 1024;

var mm = new MemoryManager(memory);

process.on('message', function (msg) {
	if (typeof msg === 'object' && msg.req) {
		switch (msg.req.cmd) {
		case 'runTask':
			runTask(msg);
			break;
		}
	}
});

function runTask(msg) {
	var task = parseTask(msg.req.args);
	task.mm = mm;
	task.lib = {fs: fs, Lines: Lines, mkdirp: mkdirp, mm: mm, readSplit: readSplit,uuid: uuid};
	task.run(function (result) {
		delete msg.req.args;
		msg.result = result;
		process.send(msg);
	});
}

function parseTask(str) {
	return JSON.parse(str, function(key, value) {
		if (typeof value == 'string') {
			// String value can be a regular function or an ES6 arrow function
			if (value.substring(0, 8) == 'function') {
				var args = value.match(/\(([^)]*)/)[1];
				var body = value.replace(/^function\s*[^)]*\)\s*{/, '').replace(/}$/, '');
				value = new Function(args, body);
			} else if (value.match(/^\s*\(\s*[^(][^)]*\)\s*=>/) || value.match(/^\s*\w+\s*=>/))
				value = ('indirect', eval)(value);
		}
		return value;
	});
}

function MemoryManager(memory) {
	var Kb = 1024, Mb = 1024 * Kb;
	var MAX_MEMORY = (memory - 100) * Mb;
	var maxStorageMemory = MAX_MEMORY * 0.4;
	var maxShuffleMemory = MAX_MEMORY * 0.2;
	var maxCollectMemory = MAX_MEMORY * 0.2;

	this.storageMemory = 0;
	this.shuffleMemory = 0;
	this.collectMemory = 0;
	this.sizeOf = sizeOf;

	this.storageFull = function () {return (this.storageMemory > maxStorageMemory);};
	this.shuffleFull = function () {return (this.shuffleMemory > maxShuffleMemory);};
	this.collectFull = function () {return (this.collectMemory > maxCollectMemory);};

	this.partitions = {};
	this.register = function (partition) {
		var key = partition.datasetId + '.' + partition.partitionIndex;
		if (!(key in this.partitions)) this.partitions[key] = partition;
	};

	this.unregister = function (partition) {
		this.partitions[partition.datasetId + '.' + partition.partitionIndex] = undefined;
	};

	this.isAvailable = function (partition) {
		return (this.partitions[partition.datasetId + '.' + partition.partitionIndex] != undefined);
	};
}

/*
function log() {
	var args = Array.prototype.slice.call(arguments);
	args.unshift('[worker ' + process.argv[2] + ']');
	console.log.apply(null, args);
}
*/
