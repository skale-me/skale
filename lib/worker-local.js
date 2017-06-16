// Copyright 2016 Luca-SAS, licensed under the Apache License 2.0

// worker module

'use strict';

var fs = require('fs');
var zlib = require('zlib');
var url = require('url');
var stream = require('stream');

var mkdirp = require('mkdirp');
var uuid = require('uuid');
var aws = require('aws-sdk');
var azure = require('azure-storage');
var parquet = require('./stub-parquet.js');

var Dataset = require('./dataset.js');
var Task = require('./task.js');
var sizeOf = require('./rough-sizeof.js');
var Lines = require('./lines.js');
var readSplit = require('./readsplit.js').readSplit;

var workerId = process.argv[2];
var memory = process.argv[3];

var mm = new MemoryManager(memory);

var start = Date.now();

if (process.env.SKALE_RANDOM_SEED)
  Dataset.setRandomSeed(process.env.SKALE_RANDOM_SEED);

process.title = 'skale-worker-' + workerId;

process.on('disconnect', function () {
  log('disconnected, exit');
  process.exit();
});

process.on('message', function (msg) {
  if (typeof msg === 'object' && msg.req) {
    switch (msg.req.cmd) {
    case 'runTask':
      runTask(msg);
      break;
    case 'runztask':
      runztask(msg);
      break;
    }
  }
});

function runztask(msg) {
  var file = msg.req.args;
  fs.readFile(file, function (err, data) {
    fs.unlink(file, function () {});
    if (err) throw new Error(err);
    zlib.gunzip(data, {chunkSize: 65536}, function (err, data) {
      if (err) throw new Error(err);
      msg.req.args = data;
      runTask(msg);
    });
  });
}

function runTask(msg) {
  var task = parseTask(msg.req.args);
  task.workerId = workerId;
  task.grid = {host: {}};
  task.mm = mm;
  task.lib = {aws: aws, azure: azure, fs: fs, Lines: Lines, mkdirp: mkdirp, mm: mm, parquet: parquet, readSplit: readSplit, stream: stream, url: url, uuid: uuid, zlib: zlib};
  task.log = log;
  task.dlog = dlog;
  task.run(function (result) {
    delete msg.req.args;
    msg.result = result;
    msg.result.workerId = workerId;
    process.send(msg);
  });
}

function parseTask(str) {
  var i, j, n, ref;
  var task = JSON.parse(str, function (key, value) {
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

  for (i in task.nodes) {
    n = task.nodes[i];
    for (j in n.dependencies) {
      ref = n.dependencies[j];
      n.dependencies[j] = task.nodes[ref];
    }
    for (j in n.partitions) {
      Object.setPrototypeOf(task.nodes[i].partitions[j], Dataset.Partition.prototype);
      task.nodes[i].partitions[j].count = 0;
      task.nodes[i].partitions[j].bsize = 0;
      task.nodes[i].partitions[j].tsize = 0;
      task.nodes[i].partitions[j].skip = false;
    }
    if (n.type) {
      Object.setPrototypeOf(task.nodes[i], Dataset[n.type].prototype);
    }
    if (n.partitioner && n.partitioner.type) {
      Object.setPrototypeOf(n.partitioner, Dataset[n.partitioner.type].prototype);
    }
  }
  Object.setPrototypeOf(task, Task.prototype);
  //log('task:', JSON.stringify(task, null, 2));
  return task;
}

function MemoryManager(memory) {
  memory = memory || 1024;
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

if (process.env.SKALE_DEBUG > 1) {
  var log =  function() {
    var args = Array.prototype.slice.call(arguments);
    args.unshift('[worker-' +  process.argv[2] + ' ' + (Date.now() - start) / 1000 + 's]');
    console.error.apply(null, args);
  };
  var dlog = function() {
    var args = Array.prototype.slice.call(arguments);
    var now = Date.now();
    var lstart = args.shift();
    args.unshift('[worker-' +  process.argv[2] + ' ' + (now - start) / 1000 + 's]');
    args.push('in ' + (now - lstart) / 1000 + 's');
    console.error.apply(null, args);
  };
} else {
  dlog = log = function () {};
}
